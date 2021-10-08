package torr.peerroutines

import zio._
import torr.metainfo.MetaInfo
import torr.peerwire.MessageTypes._
import torr.peerwire.{Message, PeerHandle, TorrBitSet}
import zio.clock.Clock
import zio.duration.durationInt

import scala.collection.immutable.HashSet
import torr.directbuffers.DirectBufferPool
import torr.dispatcher.{AcquireJobResult, Dispatcher, DownloadJob}
import torr.fileio.FileIO
import zio.logging.Logging

object DefaultPeerRoutine {

  final case class DownloadState(peerChoking: Boolean, amInterested: Boolean)
  final case class UploadState(amChoking: Boolean, peerInterested: Boolean)

  def run(peerHandle: PeerHandle): RIO[Dispatcher with FileIO with DirectBufferPool with Logging with Clock, Unit] = {
    for {
      metaInfo    <- FileIO.metaInfo
      localBitSet <- Dispatcher.getLocalBitField
      _           <- peerHandle.send(Message.BitField(localBitSet))

      bitField <- peerHandle.receive[BitField]
      _        <- validateRemoteBitSet(metaInfo, bitField.bits)
      _        <- Dispatcher.reportHaveMany(peerHandle.peerId, Set.from(bitField.bits.set))

      downSpeedAccRef <- Ref.make(0L)
      upSpeedAccRef   <- Ref.make(0L)

      haveFib  <- handleRemoteHave(peerHandle, metaInfo).fork
      aliveFib <- handleKeepAlive(peerHandle).fork
      speedFib <- reportSpeeds(peerHandle, downSpeedAccRef, upSpeedAccRef).fork

      _ <- PipelineDownloadRoutine.download(
             peerHandle,
             DownloadState(peerChoking = true, amInterested = false),
             downSpeedAccRef
           ).onError(e => Logging.debug(s"PipelineDownloadRoutine failed with\n${e.prettyPrint}"))

      _ <- haveFib.interrupt
      _ <- aliveFib.interrupt
      _ <- speedFib.interrupt

      _ <- Logging.debug(s"${peerHandle.peerIdStr} exited")
    } yield ()
  }

  private[peerroutines] def handleRemoteHave(
      peerHandle: PeerHandle,
      metaInfo: MetaInfo
  ): RIO[Dispatcher, Unit] = {
    for {
      have <- peerHandle.receive[Have]
      _    <- validatePieceIndex(metaInfo, have.pieceIndex)
      _    <- Dispatcher.reportHave(peerHandle.peerId, have.pieceIndex)
      _    <- handleRemoteHave(peerHandle, metaInfo)
    } yield ()
  }

  private[peerroutines] def handleKeepAlive(peerHandle: PeerHandle): Task[Unit] = {
    for {
      _ <- peerHandle.receive[KeepAlive]
      _ <- peerHandle.send(Message.KeepAlive)
      _ <- handleKeepAlive(peerHandle)
    } yield ()
  }

  private def reportSpeeds(
      peerHandle: PeerHandle,
      downSpeedAccRef: Ref[Long],
      upSpeedAccRef: Ref[Long],
      periodSeconds: Int = 1
  ): RIO[Dispatcher with Clock, Unit] = {
    for {
      dl <- downSpeedAccRef.getAndSet(0L)
      ul <- upSpeedAccRef.getAndSet(0L)

      dlSpeed = (dl.toDouble / periodSeconds).toInt
      ulSpeed = (ul.toDouble / periodSeconds).toInt

      _ <- Dispatcher.reportDownloadSpeed(peerHandle.peerId, dlSpeed)
      _ <- Dispatcher.reportUploadSpeed(peerHandle.peerId, ulSpeed)

      _ <- reportSpeeds(peerHandle, downSpeedAccRef, upSpeedAccRef, periodSeconds)
             .delay(periodSeconds.seconds)
    } yield ()
  }

  //noinspection SimplifyUnlessInspection,SimplifyWhenInspection
  private def validateRemoteBitSet(metaInfo: MetaInfo, bitSet: TorrBitSet): Task[Unit] = {
    if (metaInfo.pieceHashes.length > bitSet.length)
      ZIO.fail(new ProtocolException(
        s"metaInfo.pieceHashes.length(${metaInfo.pieceHashes.length}) > bitSet.length(${bitSet.length})"
      ))
    else ZIO.unit
  }

  //noinspection SimplifyWhenInspection
  private def validatePieceIndex(metaInfo: MetaInfo, pieceIndex: Int): Task[Unit] = {
    if (pieceIndex < 0 || pieceIndex >= metaInfo.pieceHashes.length)
      ZIO.fail(new ProtocolException(s"pieceIndex < 0 || pieceIndex >= metaInfo.pieceHashes.length ($pieceIndex)"))
    else ZIO.unit
  }
}
