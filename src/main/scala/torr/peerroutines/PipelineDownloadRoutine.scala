package torr.peerroutines

import torr.directbuffers.DirectBufferPool
import torr.dispatcher.{AcquireJobResult, Dispatcher, DownloadJob, PieceId, ReleaseJobStatus}
import torr.fileio.FileIO
import torr.peerroutines.DefaultPeerRoutine.DownloadState
import torr.peerwire.MessageTypes.{Choke, Piece, Unchoke}
import torr.peerwire.{Message, PeerHandle}
import zio._
import zio.clock.Clock
import zio.duration.durationInt

import scala.collection.immutable

object PipelineDownloadRoutine {

  final case class SentRequest(job: DownloadJob, request: Message.Request)

  sealed trait SenderResult
  object SenderResult {
    case object NotInterested extends SenderResult
    case object ChokedOnQueue extends SenderResult
  }

  sealed trait ReceiverResult
  object ReceiverResult {
    case class Choked(job: DownloadJob) extends ReceiverResult
    case object Completed               extends ReceiverResult
  }

  def download(
      peerHandle: PeerHandle,
      state: DownloadState,
      maxConcurrentRequests: Int
  ): RIO[Dispatcher with FileIO with DirectBufferPool with Clock, Unit] = {
    Dispatcher.isDownloadCompleted.flatMap {
      case true  => ZIO.unit
      case false => Dispatcher.isRemoteInteresting(peerHandle.peerId).flatMap {
          // Download is not completed and remote peer does not have pieces that we are interested in.
          case false => download(peerHandle, state, maxConcurrentRequests).delay(10.seconds)
          case _     => downloadWhileInterested(peerHandle, state, maxConcurrentRequests)
        }

    }
  }

  def downloadWhileInterested(
      peerHandle: PeerHandle,
      state: DownloadState,
      maxConcurrentRequests: Int
  ): RIO[Dispatcher with FileIO with DirectBufferPool with Clock, Unit] = {
    for {
      _ <- peerHandle.send(Message.Interested).unless(state.amInterested)
      _ <- peerHandle.receive[Unchoke].when(state.peerChoking) // choked = false(?), interested = true

      // DownloadJob that is currently allocated by the sendRequests fiber.
      // If we interrupt this fiber we are responsible for releasing the job.
      sndJobOpt <- Ref.make[Option[DownloadJob]](None)
      requests  <- Queue.bounded[Option[SentRequest]](maxConcurrentRequests)

      sender    <- sendRequests(peerHandle, requests, sndJobOpt, beforeFirstRequest = true)
                     // We must terminate receiver by enqueuing None if sender fails unexpectedly.
                     .onError(e => (ZIO(println(e)) *> requests.offer(None)).ignore)
                     .fork

      rcvResult <- receiveResponses(peerHandle, requests)

      _ <- rcvResult match {
             // Remote peer has choked us. Sender is probably blocked by queue.Offer call.
             // Remote peer won't respond to any of the pending requests.
             case ReceiverResult.Choked(rcvJob) =>
               for {
                 _ <- sender.interrupt
                 // receiveResponses function is responsible for releasing jobs.
                 // However it is only aware of jobs that have arrived from the `requests` queue.
                 // If there is a job that had been allocated by the sender and has not been seen by the
                 // receiveResponses function we have to release it after interruption of the sender.
                 _ <- sndJobOpt.get.flatMap {
                        case Some(sndJob) =>
                          Dispatcher.releaseJob(peerHandle.peerId, ReleaseJobStatus.Choked(sndJob))
                            .unless(sndJob.pieceId == rcvJob.pieceId)
                        case None         => ZIO.unit
                      }
                 _ <- downloadWhileInterested(
                        peerHandle,
                        DownloadState(amInterested = true, peerChoking = true),
                        maxConcurrentRequests
                      )
               } yield ()

             // This means that the receiver has successfully processed all responses to requests sent by the sender.
             // To find out the reason why sender has stopped sending requests, we must inspect the sender's return value.
             case ReceiverResult.Completed      =>
               for {
                 sndResult <- sender.join
                 _         <- sndResult match {
                                // Remote peer does not have pieces that are interesting to us.
                                case SenderResult.NotInterested =>
                                  peerHandle.send(Message.NotInterested) *>
                                    download(
                                      peerHandle,
                                      DownloadState(amInterested = false, peerChoking = false),
                                      maxConcurrentRequests
                                    )

                                case SenderResult.ChokedOnQueue =>
                                  download(
                                    peerHandle,
                                    DownloadState(amInterested = true, peerChoking = true),
                                    maxConcurrentRequests
                                  )
                              }
               } yield ()
           }
    } yield ()
  }

  //noinspection SimplifyUnlessInspection
  private def sendRequests(
      handle: PeerHandle,
      requestQueue: Queue[Option[SentRequest]],
      currentJobRef: Ref[Option[DownloadJob]],
      beforeFirstRequest: Boolean,
      requestSize: Int = 16 * 1024
  ): RIO[Dispatcher, SenderResult] = {

    def sendRequestsForJob(job: DownloadJob, offset: Int): Task[Unit] = {
      val remaining = job.length - offset
      if (remaining <= 0) ZIO.unit
      else {
        val amount  = math.min(requestSize, remaining)
        val request = Message.Request(job.pieceId, offset, amount)
        for {
          _ <- handle.send(request)
          _ <- requestQueue.offer(Some(SentRequest(job, request)))
          _ <- sendRequestsForJob(job, offset + amount)
        } yield ()
      }
    }

    Dispatcher.acquireJob(handle.peerId).flatMap {

      case AcquireJobResult.AcquireSuccess(job) if beforeFirstRequest =>
        handle.poll[Choke].flatMap {
          case None    =>
            currentJobRef.set(Some(job)) *>
              sendRequestsForJob(job, 0) *>
              sendRequests(handle, requestQueue, currentJobRef, beforeFirstRequest = false)
          case Some(_) =>
            currentJobRef.set(None) *>
              Dispatcher.releaseJob(handle.peerId, ReleaseJobStatus.Choked(job)) *>
              requestQueue.offer(None).as(SenderResult.ChokedOnQueue)
        }

      case AcquireJobResult.AcquireSuccess(job)                       =>
        currentJobRef.set(Some(job)) *>
          sendRequestsForJob(job, 0) *>
          sendRequests(handle, requestQueue, currentJobRef, beforeFirstRequest)

      case AcquireJobResult.NotInterested                             =>
        currentJobRef.set(None) *>
          requestQueue.offer(None).as(SenderResult.NotInterested)
    }

  }

  /**
    * For each sent request in `requestQueue` receives and processes one response from the remote peer.
    * If responses appear out of order, it reorders them by storing each response in `receivedResponses`.
    * @return Last job that has been dequeued from the `requestQueue`.
    */
  private def receiveResponses(
      peerHandle: PeerHandle,
      requestQueue: Queue[Option[SentRequest]],
      lastSeenJob: Option[DownloadJob] = None,      // Last job that has been dequeued from the requestQueue.
      lastProcessedJob: Option[DownloadJob] = None, // Last job a request has been processed from.
      // Response reordering buffers.
      requestsInOrder: immutable.Queue[SentRequest] = immutable.Queue.empty[SentRequest],
      receivedResponses: immutable.HashMap[(Int, Int), Message.Piece] =
        immutable.HashMap.empty[(Int, Int), Message.Piece]
  ): RIO[Dispatcher with FileIO, ReceiverResult] = {

    // Next response in order to process if available.
    val nextResponseInOrderOption = for {
      req  <- requestsInOrder.headOption
      key   = (req.request.index, req.request.offset)
      resp <- receivedResponses.get(key)
    } yield (resp, key, req)

    if (nextResponseInOrderOption.isDefined) {
      val (response, responseKey, request) = nextResponseInOrderOption.get

      for {
        // If the response being processed is related to the next job, we must release the previous one.
        _   <- Dispatcher.releaseJob(peerHandle.peerId, ReleaseJobStatus.Active(lastProcessedJob.get))
                 .when(lastProcessedJob.isDefined && lastProcessedJob.get.pieceId != request.job.pieceId)
        _   <- validateResponse(response, request.request)
        _   <- request.job.hashBlock(response.offset, response.block)
        _   <- FileIO.store(response.index, response.offset, Chunk(response.block))
        res <- receiveResponses(
                 peerHandle,
                 requestQueue,
                 lastSeenJob,
                 Some(request.job),
                 requestsInOrder.tail,
                 receivedResponses - responseKey
               )
      } yield res
    } else {
      // We don't have any responses to process in order so let's receive another one.
      requestQueue.take.flatMap {

        case None       =>
          // We must have received all responses already.
          // So there must be enough responses to process them in order.
          if (requestsInOrder.nonEmpty || receivedResponses.nonEmpty) {
            ZIO.fail(new ProtocolException(s"Unable to correctly reorder received responses"))
          } else {
            lastSeenJob match {
              case Some(job) =>
                Dispatcher.releaseJob(peerHandle.peerId, ReleaseJobStatus.Active(job)) *>
                  ZIO.succeed(ReceiverResult.Completed)
              case None      => ZIO.succeed(ReceiverResult.Completed)
            }
          }

        case Some(sent) =>
          peerHandle.receive[Piece, Choke].flatMap { // TODO timeoutFail

            case resp @ Message.Piece(_, _, _) =>
              val responseKey = (resp.index, resp.offset)
              receiveResponses(
                peerHandle,
                requestQueue,
                Some(sent.job),
                lastProcessedJob,
                requestsInOrder :+ sent,
                receivedResponses + (responseKey -> resp)
              )

            case Message.Choke                 =>
              val currentlyProcessedJobs = requestsInOrder.map(_.job).distinct.toSet + sent.job
              for {
                _ <- ZIO.foreach_(currentlyProcessedJobs)(j =>
                       Dispatcher.releaseJob(
                         peerHandle.peerId,
                         ReleaseJobStatus.Choked(j)
                       )
                     )
              } yield ReceiverResult.Choked(sent.job)

            case _                             => ???
          }
      }
    }
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
}
