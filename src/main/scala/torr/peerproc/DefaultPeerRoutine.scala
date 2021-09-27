package torr.peerproc

import zio._
import torr.metainfo.MetaInfo
import torr.peerwire.MessageTypes._
import torr.peerwire.{Message, PeerHandle, TorrBitSet}
import zio.clock.Clock
import zio.duration.durationInt
import scala.collection.immutable.HashSet
import torr.directbuffers.DirectBufferPool
import torr.dispatcher.{AcquireSuccess, Dispatcher, DownloadJob, NotInterested}
import torr.fileio.FileIO

import scala.collection.immutable

object DefaultPeerRoutine {

  final case class DownloadState(peerChoking: Boolean, amInterested: Boolean)
  final case class UploadState(amChoking: Boolean, peerInterested: Boolean)

  def run(peerHandle: PeerHandle): RIO[Dispatcher with FileIO with DirectBufferPool with Clock, Unit] = {
    for {
      remoteHaveRef <- Ref.make(Set.empty[Int])
      metaInfo      <- FileIO.metaInfo
      haveFib       <- handleRemoteHave(peerHandle, metaInfo, remoteHaveRef).fork
      aliveFib      <- handleKeepAlive(peerHandle).fork

      _ <- PipelineDownloadRoutine.download(
             peerHandle,
             remoteHaveRef,
             DownloadState(peerChoking = true, amInterested = false),
             maxConcurrentRequests = 128
           )

      _ <- haveFib.interrupt
      _ <- aliveFib.interrupt
    } yield ()
  }

  private[peerproc] def handleRemoteHave(
      peerHandle: PeerHandle,
      metaInfo: MetaInfo,
      remoteHaveRef: Ref[Set[Int]]
  ): Task[Unit] = {
    for {
      msg <- peerHandle.receive[BitField, Have]
      _   <- msg match {
               case Message.BitField(bitSet) =>
                 for {
                   _ <- validateRemoteBitSet(metaInfo, bitSet)
                   _ <- remoteHaveRef.set(HashSet.from(bitSet.set))
                 } yield ()
               case Message.Have(pieceIndex) =>
                 for {
                   _ <- validatePieceIndex(metaInfo, pieceIndex)
                   _ <- remoteHaveRef.update(s => s + pieceIndex)
                 } yield ()
               case _                        => ???
             }
      _   <- handleRemoteHave(peerHandle, metaInfo, remoteHaveRef)
    } yield ()
  }

  private[peerproc] def handleKeepAlive(peerHandle: PeerHandle): Task[Unit] = {
    for {
      _ <- peerHandle.receive[KeepAlive]
      _ <- peerHandle.send(Message.KeepAlive)
      _ <- handleKeepAlive(peerHandle)
    } yield ()
  }

  //noinspection SimplifyUnlessInspection
  private def validateRemoteBitSet(metaInfo: MetaInfo, bitSet: TorrBitSet): Task[Unit] = {
    if (metaInfo.pieceHashes.length != bitSet.length)
      ZIO.fail(new ProtocolException("metaInfo.pieceHashes.length != torrBitSet.length"))
    else ZIO.unit
  }

  //noinspection SimplifyWhenInspection
  private def validatePieceIndex(metaInfo: MetaInfo, pieceIndex: Int): Task[Unit] = {
    if (pieceIndex < 0 || pieceIndex >= metaInfo.pieceHashes.length)
      ZIO.fail(new ProtocolException(s"pieceIndex < 0 || pieceIndex >= metaInfo.pieceHashes.length ($pieceIndex)"))
    else ZIO.unit
  }
}
