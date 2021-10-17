package torr.peerroutines

import torr.directbuffers.DirectBufferPool
import torr.dispatcher.{Dispatcher, PieceId}
import torr.fileio.FileIO
import torr.metainfo.MetaInfo
import torr.peerwire.MessageTypes._
import torr.peerwire.{Message, PeerHandle, TorrBitSet}
import zio._
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.Logging

import scala.collection.immutable

object DefaultPeerRoutine {

  def run(peerHandle: PeerHandle): RIO[Dispatcher with FileIO with DirectBufferPool with Logging with Clock, Unit] = {
    for {
      metaInfo <- FileIO.metaInfo

      (initialHave, haveQueue) <- Dispatcher.subscribeToHaveUpdates(peerHandle.peerId)

      localHaveRef <- Ref.make(initialHave)

      _               <- peerHandle.send(
                           Message.BitField(
                             TorrBitSet.fromSet(initialHave, metaInfo.pieceHashes.length)
                           )
                         )

      downSpeedAccRef <- Ref.make(0)
      upSpeedAccRef   <- Ref.make(0)

      bitFieldReceivedSignal <- Promise.make[Nothing, Unit]

      aliveFib      <- handleKeepAlive(peerHandle).fork
      speedFib      <- reportSpeeds(peerHandle, downSpeedAccRef, upSpeedAccRef).fork
      localHaveFib  <- handleLocalHaveUpdates(peerHandle, localHaveRef, haveQueue).fork
      remoteHaveFib <- handleRemoteHave(peerHandle, metaInfo, bitFieldReceivedSignal).fork
      _             <- bitFieldReceivedSignal.await.timeout(1.second)

      uploadFib <- DefaultUploadRoutine.upload(peerHandle, metaInfo, localHaveRef, upSpeedAccRef).fork

      _ <- DefaultDownloadRoutine.restart(
             peerHandle,
             downSpeedAccRef
           )

      _ <- Logging.debug(s"${peerHandle.peerIdStr} DefaultDownloadRoutine exited")

      _ <- remoteHaveFib.interrupt
      _ <- localHaveFib.interrupt
      _ <- aliveFib.interrupt
      _ <- speedFib.interrupt
      _ <- uploadFib.interrupt

      _ <- Logging.debug(s"${peerHandle.peerIdStr} exited")
    } yield ()
  }

  private[peerroutines] def handleRemoteHave(
      peerHandle: PeerHandle,
      metaInfo: MetaInfo,
      bitFieldReceivedSignal: Promise[Nothing, Unit]
  ): RIO[Dispatcher, Unit] = {

    peerHandle.receive[Have, BitField].flatMap {
      case Message.Have(pieceId)  =>
        for {
          _ <- validatePieceIndex(metaInfo, pieceId)
          _ <- Dispatcher.reportHave(peerHandle.peerId, pieceId)
          _ <- handleRemoteHave(peerHandle, metaInfo, bitFieldReceivedSignal)
        } yield ()

      case Message.BitField(bits) =>
        for {
          _ <- validateRemoteBitSet(metaInfo, bits)
          _ <- Dispatcher.reportHaveMany(peerHandle.peerId, Set.from(bits.set))
          _ <- bitFieldReceivedSignal.succeed().unlessM(bitFieldReceivedSignal.isDone)
        } yield ()

      case _                      => ???
    } *> handleRemoteHave(peerHandle, metaInfo, bitFieldReceivedSignal)
  }

  private def handleLocalHaveUpdates(
      peerHandle: PeerHandle,
      localHaveRef: Ref[Set[PieceId]],
      haveQueue: Dequeue[PieceId]
  ): Task[Unit] = {
    for {
      pieceId <- haveQueue.take
      _       <- peerHandle.send(Message.Have(pieceId))
      _       <- localHaveRef.update(s => s + pieceId)
      _       <- handleLocalHaveUpdates(peerHandle, localHaveRef, haveQueue)
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
      downSpeedAccRef: Ref[Int],
      upSpeedAccRef: Ref[Int],
      periodSeconds: Int = 1
  ): RIO[Dispatcher with Clock, Unit] = {

    def loop(
        downSpeeds: immutable.Queue[Int],
        upSpeeds: immutable.Queue[Int]
    ): RIO[Dispatcher with Clock, Unit] =
      for {
        dl <- downSpeedAccRef.getAndSet(0)
        ul <- upSpeedAccRef.getAndSet(0)

        dlInstantSpeed = (dl.toDouble / periodSeconds).toInt
        ulInstantSpeed = (ul.toDouble / periodSeconds).toInt

        dlSpeed = downSpeeds.sum / downSpeeds.length
        ulSpeed = upSpeeds.sum / upSpeeds.length

        _ <- Dispatcher.reportDownloadSpeed(peerHandle.peerId, dlSpeed)
        _ <- Dispatcher.reportUploadSpeed(peerHandle.peerId, ulSpeed)

        _ <- loop(
               downSpeeds.tail.enqueue(dlInstantSpeed),
               upSpeeds.tail.enqueue(ulInstantSpeed)
             ).delay(periodSeconds.seconds)
      } yield ()

    loop(
      immutable.Queue[Int]((1 to 10).map(_ => 0): _*),
      immutable.Queue[Int]((1 to 10).map(_ => 0): _*)
    )
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
