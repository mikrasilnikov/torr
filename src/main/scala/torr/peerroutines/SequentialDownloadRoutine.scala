package torr.peerroutines

import torr.directbuffers.DirectBufferPool
import torr.dispatcher.{AcquireJobResult, Dispatcher, DownloadJob}
import torr.fileio.FileIO
import torr.metainfo.MetaInfo
import torr.peerroutines.DefaultPeerRoutine.DownloadState
import torr.peerwire.MessageTypes.{BitField, Choke, Have, KeepAlive, Piece, Unchoke}
import torr.peerwire.{Message, PeerHandle, TorrBitSet}
import zio.{Chunk, RIO, Ref, Task, ZIO}
import zio.clock.Clock
import zio.duration.durationInt

import scala.collection.immutable
import scala.collection.immutable.HashSet

object SequentialDownloadRoutine {

  def download(
      peerHandle: PeerHandle,
      remoteHaveRef: Ref[Set[Int]],
      state: DownloadState,
      maxConcurrentRequests: Int
  ): RIO[Dispatcher with FileIO with DirectBufferPool with Clock, Unit] = {
    Dispatcher.isDownloadCompleted.flatMap {
      case true  => ZIO.unit
      case false => Dispatcher.isRemoteInteresting(peerHandle.peerId).flatMap {
          // Download is not completed and remote peer does not have pieces that we are interested in.
          case false => download(peerHandle, remoteHaveRef, state, maxConcurrentRequests).delay(10.seconds)
          case _     =>
            for {
              _ <- peerHandle.send(Message.Interested).unless(state.amInterested)
              _ <- peerHandle.receive[Unchoke].when(state.peerChoking)

              // choked = false(?), interested = true
              state1 <- Dispatcher.acquireJobManaged(peerHandle.peerId).use {
                          case AcquireJobResult.Success(job)        =>
                            downloadUntilChokedOrCompleted(peerHandle, job, maxConcurrentRequests)
                              .map(choked => DownloadState(choked, amInterested = true))

                          case AcquireJobResult.NoInterestingPieces =>
                            for {
                              _ <- peerHandle.send(Message.NotInterested)
                              _ <- ZIO.sleep(10.seconds)
                            } yield DownloadState(peerChoking = false, amInterested = false)
                        }
              _      <- download(peerHandle, remoteHaveRef, state1, maxConcurrentRequests)
            } yield ()

        }
    }
  }

  /**
    * @return true if being choked by remote peer
    */
  //noinspection SimplifyUnlessInspection
  private[peerroutines] def downloadUntilChokedOrCompleted(
      peerHandle: PeerHandle,
      job: DownloadJob,
      maxConcurrentRequests: Int,
      requestSize: Int = 16 * 1024
  ): RIO[FileIO with DirectBufferPool, Boolean] = {

    def loop(
        requestOffset: Int = 0,
        pendingRequests: immutable.Queue[Message.Request] = immutable.Queue.empty[Message.Request]
    ): RIO[FileIO with DirectBufferPool, Boolean] = {
      val remaining = job.length - requestOffset

      if (remaining <= 0 && pendingRequests.isEmpty) {
        ZIO.succeed(false)

      } else if (remaining > 0 && pendingRequests.size < maxConcurrentRequests) {
        // We must maintain a queue of unfulfilled requests for performance reasons.
        // See https://wiki.theory.org/BitTorrentSpecification#Queuing

        // We should not send requests if remote peer has choked us.
        peerHandle.poll[Choke].flatMap {
          case Some(_) =>
            ZIO.succeed(true)
          case None    =>
            val amount  = math.min(requestSize, remaining)
            val request = Message.Request(job.pieceId, requestOffset, amount)
            for {
              _   <- peerHandle.send(request)
              res <- loop(
                       requestOffset + amount,
                       pendingRequests :+ request
                     )
            } yield res
        }
      } else {
        for {
          msg <- peerHandle.receive[Piece, Choke]
          res <- msg match {
                   case Message.Choke => ZIO.succeed(true)

                   case response @ Message.Piece(pieceIndex, offset, data) =>
                     for {
                       _   <- validateResponse(response, pendingRequests.head)
                       _   <- job.hashBlock(offset, data)
                       _   <- FileIO.store(pieceIndex, offset, Chunk(data))
                       res <- loop(requestOffset, pendingRequests.tail)
                     } yield res

                   case _                                                  => ???
                 }
        } yield res
      }
    }

    loop()
  }

  private def validateResponse(response: Message.Piece, request: Message.Request): Task[Unit] = {
    for {
      dataSize <- response.block.remaining
      _        <- ZIO.fail(new ProtocolException(
                    s"Received piece does not correspond to sent request. $response $request"
                  ))
                    .unless(
                      response.index == request.index &&
                        response.offset == request.offset &&
                        dataSize == request.length
                    )
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
