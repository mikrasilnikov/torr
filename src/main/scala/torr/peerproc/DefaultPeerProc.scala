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

object DefaultPeerProc {

  final case class DownloadState(peerChoking: Boolean, amInterested: Boolean)
  final case class UploadState(amChoking: Boolean, peerInterested: Boolean)

  def run(peerHandle: PeerHandle): RIO[Dispatcher with FileIO with DirectBufferPool with Clock, Unit] = {
    for {
      remoteHaveRef <- Ref.make(Set.empty[Int])
      metaInfo      <- FileIO.metaInfo
      haveFib       <- handleRemoteHave(peerHandle, metaInfo, remoteHaveRef).fork
      aliveFib      <- handleKeepAlive(peerHandle).fork

      _ <- download(peerHandle, remoteHaveRef, DownloadState(peerChoking = true, amInterested = false))

      _ <- haveFib.interrupt
      _ <- aliveFib.interrupt
    } yield ()
  }

  private[peerproc] def download(
      peerHandle: PeerHandle,
      remoteHaveRef: Ref[Set[Int]],
      state: DownloadState
  ): RIO[Dispatcher with FileIO with DirectBufferPool with Clock, Unit] = {
    Dispatcher.isDownloadCompleted.flatMap {
      case true  => ZIO.unit
      case false =>
        remoteHaveRef.get.flatMap { remoteHave =>
          Dispatcher.isRemoteInteresting(remoteHave).flatMap {
            // Download is not completed and remote peer does not have pieces that we are interested in.
            case false => download(peerHandle, remoteHaveRef, state).delay(10.seconds)
            case _     =>
              for {
                _ <- peerHandle.send(Message.Interested).unless(state.amInterested)
                _ <- peerHandle.receive[Unchoke].when(state.peerChoking)

                // choked = false(?), interested = true
                s1 <- Dispatcher.acquireJobManaged(remoteHave).use {
                        case AcquireSuccess(job) =>
                          downloadUntilChokedOrCompleted(peerHandle, job)
                            .map(choked => DownloadState(choked, amInterested = true))

                        case NotInterested       =>
                          for {
                            _ <- peerHandle.send(Message.NotInterested)
                            _ <- ZIO.sleep(10.seconds)
                          } yield DownloadState(peerChoking = false, amInterested = false)
                      }
                _  <- download(peerHandle, remoteHaveRef, s1)
              } yield ()
          }
        }
    }
  }

  /**
    * @return true if being choked by remote peer
    */
  //noinspection SimplifyUnlessInspection
  private[peerproc] def downloadUntilChokedOrCompleted(
      peerHandle: PeerHandle,
      job: DownloadJob,
      concurrentRequests: Int = 128,
      requestSize: Int = 16 * 1024
  ): RIO[FileIO with DirectBufferPool, Boolean] = {

    def loop(
        requestOffset: Int = 0,
        pendingRequests: immutable.Queue[Message.Request] = immutable.Queue.empty[Message.Request]
    ): RIO[FileIO with DirectBufferPool, Boolean] = {
      val remaining = job.pieceLength - requestOffset

      if (remaining <= 0 && pendingRequests.isEmpty) {
        ZIO.succeed(false)

      } else if (remaining > 0 && pendingRequests.size < concurrentRequests) {
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

  private[peerproc] def waitUntilRemoteIsInteresting(
      peerHandle: PeerHandle,
      remoteHaveRef: Ref[Set[Int]]
  ): RIO[Dispatcher with Clock, Unit] = {
    for {
      remoteHave  <- remoteHaveRef.get
      interesting <- Dispatcher.isRemoteInteresting(remoteHave)
      _           <- interesting match {
                       case true  => ZIO.unit
                       case false => waitUntilRemoteIsInteresting(peerHandle, remoteHaveRef).delay(1.second)
                     }
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
