package torr.peerroutines

import torr.directbuffers.DirectBufferPool
import torr.dispatcher.{AcquireUploadSlotResult, Dispatcher, PieceId}
import torr.fileio.FileIO
import torr.peerwire.MessageTypes.{Interested, NotInterested, Request}
import zio._
import torr.peerwire.{Message, PeerHandle}
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.Logging

object DefaultUploadRoutine {

  def upload(
      peerHandle: PeerHandle,
      localHaveRef: Ref[Set[PieceId]],
      upSpeedAccRef: Ref[Int]
  ): RIO[Dispatcher with FileIO with DirectBufferPool with Logging with Clock, Unit] = {
    for {
      _ <- peerHandle.waitForPeerInterested
      _ <- acquireUploadSlot(peerHandle)

      _ <- peerHandle.unignore[Request]
      _ <- peerHandle.send(Message.Unchoke)

      _ <- serveRequests(peerHandle, localHaveRef, upSpeedAccRef) race holdUploadSlot(peerHandle)

      _ <- peerHandle.ignore[Request]
      _ <- Dispatcher.releaseUploadSlot(peerHandle.peerId)
      _ <- peerHandle.send(Message.Choke)

      _ <- upload(peerHandle, localHaveRef, upSpeedAccRef)

    } yield ()
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
      localHaveRef: Ref[Set[PieceId]],
      upSpeedAccRef: Ref[Int]
  ): RIO[FileIO with DirectBufferPool with Logging with Clock, Unit] = {
    peerHandle.receiveWhilePeerInterested[Request].flatMap {
      case None                                 => ZIO.unit
      case Some(req @ Message.Request(_, _, _)) =>
        serveRequest(peerHandle, req, upSpeedAccRef)
          .tapCause(c => ZIO(println(s"Serving request $req\n" ++ c.prettyPrint))) *>
          serveRequests(peerHandle, localHaveRef, upSpeedAccRef)
    }
  }

  private def serveRequest(
      peerHandle: PeerHandle,
      request: Message.Request,
      upSpeedAccRef: Ref[Int]
  ): RIO[FileIO with DirectBufferPool with Logging, Unit] = {
    for {
      data <- FileIO.fetch(request.index, request.offset, request.length)
      _    <- peerHandle.send(Message.Piece(request.index, request.offset, data.head))
      _    <- upSpeedAccRef.getAndUpdate(_ + request.length)
    } yield ()
  }
}
