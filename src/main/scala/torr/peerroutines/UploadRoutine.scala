package torr.peerroutines

import torr.directbuffers.DirectBufferPool
import torr.dispatcher.{AcquireUploadSlotResult, Dispatcher, PieceId}
import torr.fileio.FileIO
import torr.metainfo.MetaInfo
import torr.peerwire.MessageTypes.{Cancel, Request}
import torr.peerwire.{Message, PeerHandle}
import zio._
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.Logging

object UploadRoutine {

  def run(
      peerHandle: PeerHandle,
      metaInfo: MetaInfo,
      localHaveRef: Ref[Set[PieceId]],
      upSpeedAccRef: Ref[Int]
  ): RIO[Dispatcher with FileIO with DirectBufferPool with Logging with Clock, Unit] = {

    Dispatcher.peerIsSeeding(peerHandle.peerId).flatMap {
      // Remote peer has completed downloading.
      case true => ZIO.unit
      case _    =>
        for {
          _ <- peerHandle.waitForPeerInterested
          _ <- acquireUploadSlot(peerHandle)

          _ <- peerHandle.unignore[Request]
          _ <- peerHandle.ignore[Cancel]
          _ <- peerHandle.send(Message.Unchoke)

          _ <- serveRequests(peerHandle, metaInfo, localHaveRef, upSpeedAccRef)
                 .raceFirst(holdUploadSlot(peerHandle))

          _ <- peerHandle.ignore[Request]
          _ <- Dispatcher.releaseUploadSlot(peerHandle.peerId)
          _ <- peerHandle.send(Message.Choke)

          _ <- run(peerHandle, metaInfo, localHaveRef, upSpeedAccRef)

        } yield ()
    }
  }

  def acquireUploadSlot(peerHandle: PeerHandle): RIO[Dispatcher with Clock, Unit] = {
    import AcquireUploadSlotResult._
    Dispatcher.acquireUploadSlot(peerHandle.peerId).flatMap {
      case Granted => ZIO.unit
      case Denied  => acquireUploadSlot(peerHandle).delay(1.second)
    }
  }

  def holdUploadSlot(
      peerHandle: PeerHandle
  ): RIO[Dispatcher with Clock, Unit] = {
    import AcquireUploadSlotResult._
    Dispatcher.acquireUploadSlot(peerHandle.peerId)
      .flatMap {
        case Denied  => ZIO.unit
        case Granted => holdUploadSlot(peerHandle).delay(10.seconds)
      }
  }

  def serveRequests(
      peerHandle: PeerHandle,
      metaInfo: MetaInfo,
      localHaveRef: Ref[Set[PieceId]],
      upSpeedAccRef: Ref[Int]
  ): RIO[FileIO with DirectBufferPool with Logging with Clock, Unit] = {
    peerHandle.receiveWhilePeerInterested[Request].flatMap {
      case None                                 => ZIO.unit
      case Some(req @ Message.Request(_, _, _)) =>
        serveRequest(peerHandle, metaInfo, req, localHaveRef, upSpeedAccRef)
          .tapCause(c => ZIO(println(s"Serving request $req\n" ++ c.prettyPrint))) *>
          serveRequests(peerHandle, metaInfo, localHaveRef, upSpeedAccRef)
    }
  }

  private def serveRequest(
      peerHandle: PeerHandle,
      metaInfo: MetaInfo,
      request: Message.Request,
      localHaveRef: Ref[Set[PieceId]],
      upSpeedAccRef: Ref[Int]
  ): RIO[FileIO with DirectBufferPool with Logging, Unit] = {

    val requestOffset    = request.index.toLong * metaInfo.pieceSize + request.offset
    val requestRemaining = metaInfo.torrentSize - requestOffset
    val correctLength    = math.min(requestRemaining, 16 * 1024)

    for {
      localHave <- localHaveRef.get

      _    <- failWithLogging(
                peerHandle,
                new Exception(s"Remote peer requested invalid piece ${request.index}")
              ).unless(localHave.contains(request.index))

      _    <- failWithLogging(
                peerHandle,
                new Exception(s"Invalid request $request")
              ).unless(requestRemaining > 0 && request.length == correctLength)

      data <- FileIO.fetch(request.index, request.offset, request.length)
      _    <- peerHandle.send(Message.Piece(request.index, request.offset, data.head))
      _    <- upSpeedAccRef.getAndUpdate(_ + request.length)
    } yield ()
  }

  def failWithLogging(peerHandle: PeerHandle, exception: Throwable): RIO[Logging, Unit] =
    Logging.debug(s"${peerHandle.peerIdStr} serveRequest failed with ${exception.getMessage}") *>
      ZIO.fail(exception)
}
