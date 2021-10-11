package torr.peerroutines

import zio._
import zio.clock.Clock
import zio.duration.durationInt
import torr.peerwire.{Message, MessageTypes, PeerHandle}
import torr.peerroutines.DefaultPeerRoutine.DownloadState
import torr.dispatcher.{AcquireJobResult, Dispatcher, DownloadJob, PieceId, ReleaseJobStatus}
import torr.fileio.FileIO
import torr.peerwire.MessageTypes.{Choke, Piece, Unchoke}

import scala.collection.immutable.Queue
import scala.collection.immutable.HashMap

object LastDownloadRoutine {

  private val maxConcurrentRequests = 192
  private val requestSize           = 16 * 1024

  final case class SentRequest(job: DownloadJob, msg: Message.Request)

  type Offset = Int

  sealed trait CurrentJobState
  case object BeforeFirst                                        extends CurrentJobState
  case class Allocated(job: DownloadJob, var requestOffset: Int) extends CurrentJobState
  case object AfterLast                                          extends CurrentJobState

  def restart(
      peerHandle: PeerHandle,
      downloadState: DownloadState,
      downSpeedAccRef: Ref[Long]
  ): RIO[Dispatcher with FileIO with Clock, Unit] = {
    Dispatcher.isDownloadCompleted.flatMap {
      case true => ZIO.unit
      case _    => Dispatcher.isRemoteInteresting(peerHandle.peerId).flatMap {
          case false => restart(peerHandle, downloadState, downSpeedAccRef).delay(10.seconds)
          case _     => negotiateUnchoke(peerHandle, downloadState, downSpeedAccRef)
        }
    }
  }

  private def negotiateUnchoke(
      handle: PeerHandle,
      state0: DownloadState,
      downSpeedAccRef: Ref[Long]
  ): RIO[Dispatcher with FileIO with Clock, Unit] = {
    for {
      state <- updatePeerChokingState(handle, state0)

      _ <- (state.amInterested, state.peerChoking) match {
             case (false, true)  =>
               handle.send(Message.Interested) *>
                 handle.receive[Unchoke] *>
                 handle.ignore[Unchoke]

             case (false, false) =>
               handle.send(Message.Interested) *>
                 handle.ignore[Unchoke]

             case (true, true)   =>
               handle.receive[Unchoke] *>
                 handle.ignore[Unchoke]

             case (true, false)  =>
               ZIO.fail(new IllegalStateException("negotiateAndDownload: (amInterested = true, peerChoking = false)"))
           }

      _ <- continue(
             handle,
             jobState = BeforeFirst,
             requestsPerJobs = HashMap.empty[DownloadJob, Int],
             sent = Queue.empty[SentRequest],
             received = HashMap.empty[(PieceId, Int), Message.Piece],
             downSpeedAccRef
           )
    } yield ()
  }

  private def continue(
      handle: PeerHandle,
      jobState: CurrentJobState,
      requestsPerJobs: HashMap[DownloadJob, Int],
      // Response reordering buffers.
      sent: Queue[SentRequest],
      received: HashMap[(PieceId, Offset), Message.Piece],
      downSpeedAccRef: Ref[Long]
  ): RIO[Dispatcher with FileIO with Clock, Unit] = {
    allocateFirstJob(handle, jobState, requestsPerJobs, sent, received, downSpeedAccRef)
  }

  private def allocateFirstJob(
      handle: PeerHandle,
      jobState: CurrentJobState,
      requestsPerJobs: HashMap[DownloadJob, Int],
      sent: Queue[SentRequest],
      received: HashMap[(PieceId, Offset), Message.Piece],
      downSpeedAccRef: Ref[Long]
  ): RIO[Dispatcher with FileIO with Clock, Unit] = {

    if (jobState.isInstanceOf[BeforeFirst.type]) {

      Dispatcher.acquireJob(handle.peerId).flatMap {
        case AcquireJobResult.Success(job) =>
          continue(
            handle,
            jobState = Allocated(job, requestOffset = 0),
            requestsPerJobs + (job -> 0),
            sent,
            received,
            downSpeedAccRef
          )

        case _                             =>
          for {
            _ <- handle.send(Message.NotInterested)
            _ <- handle.receive[Choke].timeoutFail(new ProtocolException(
                   s"${handle.peerIdStr} has not responded with Choke after NotInterested for 5 seconds"
                 ))(5.seconds)
            _ <- restart(handle, DownloadState(peerChoking = false, amInterested = false), downSpeedAccRef)
          } yield ()
      }

    } else {
      allocateNextJob(handle, jobState, requestsPerJobs, sent, received, downSpeedAccRef)
    }
  }

  private def allocateNextJob(
      handle: PeerHandle,
      jobState: CurrentJobState,
      requestsPerJobs: HashMap[DownloadJob, Int],
      sent: Queue[SentRequest],
      received: HashMap[(PieceId, Offset), Message.Piece],
      downSpeedAccRef: Ref[Long]
  ): RIO[Dispatcher with FileIO with Clock, Unit] = {

    jobState match {
      case Allocated(job, reqOffset) if job.length - reqOffset <= 0 =>
        Dispatcher.acquireJob(handle.peerId).flatMap {
          case AcquireJobResult.Success(job) =>
            continue(
              handle,
              jobState = Allocated(job, requestOffset = 0),
              requestsPerJobs + (job -> 0),
              sent,
              received,
              downSpeedAccRef
            )

          case _                             =>
            continue(handle, jobState = AfterLast, requestsPerJobs, sent, received, downSpeedAccRef)
        }
      case _                                                        =>
        sendRequest(handle, jobState, requestsPerJobs, sent, received, downSpeedAccRef)
    }
  }

  private def sendRequest(
      handle: PeerHandle,
      jobState: CurrentJobState,
      requestsPerJobs: HashMap[DownloadJob, Int],
      sent: Queue[SentRequest],
      received: HashMap[(PieceId, Offset), Message.Piece],
      downSpeedAccRef: Ref[Long]
  ): RIO[Dispatcher with FileIO with Clock, Unit] = {

    if (
      jobState.isInstanceOf[Allocated] &&
      sent.length < maxConcurrentRequests
    ) {
      val allocated = jobState.asInstanceOf[Allocated]
      val offset    = allocated.requestOffset
      val remaining = allocated.job.length - offset

      val amount  = math.min(requestSize, remaining)
      val request = Message.Request(allocated.job.pieceId, offset, amount)
      allocated.requestOffset += amount
      val num     = requestsPerJobs(allocated.job)
      for {
        _ <- handle.send(request)
        _ <- continue(
               handle,
               allocated,
               requestsPerJobs.updated(allocated.job, num + 1),
               sent.enqueue(SentRequest(allocated.job, request)),
               received,
               downSpeedAccRef
             )
      } yield ()
    } else {
      processResponse(handle, jobState, requestsPerJobs, sent, received, downSpeedAccRef)
    }
  }

  def processResponse(
      handle: PeerHandle,
      jobState: CurrentJobState,
      requestsPerJobs: HashMap[DownloadJob, Int],
      sent: Queue[SentRequest],
      received: HashMap[(PieceId, Offset), Piece],
      downSpeedAccRef: Ref[Long]
  ): RIO[Dispatcher with FileIO with Clock, Unit] = {

    val nextResponseInOrderOption = for {
      req  <- sent.headOption
      key   = (req.msg.index, req.msg.offset)
      resp <- received.get(key)
    } yield (resp, key, req)

    if (nextResponseInOrderOption.isDefined) {
      val (response, responseKey, request) = nextResponseInOrderOption.get

      val unprocessedForJob = requestsPerJobs(request.job) - 1

      for {
        _ <- validateResponse(response, request.msg)
        _ <- request.job.hashBlock(response.offset, response.block)
        _ <- FileIO.store(response.index, response.offset, Chunk(response.block))

        _ <- downSpeedAccRef.update(_ + request.msg.length)

        _   <- Dispatcher.releaseJob(handle.peerId, ReleaseJobStatus.Downloaded(request.job))
                 .when(unprocessedForJob == 0)

        res <- continue(
                 handle,
                 jobState,
                 if (unprocessedForJob > 0) requestsPerJobs.updated(request.job, unprocessedForJob)
                 else requestsPerJobs - request.job,
                 sent.tail,
                 received - responseKey,
                 downSpeedAccRef
               )
      } yield res
    } else {
      receiveResponse(handle, jobState, requestsPerJobs, sent, received, downSpeedAccRef)
    }
  }

  private def receiveResponse(
      handle: PeerHandle,
      jobState: CurrentJobState,
      requestsPerJobs: HashMap[DownloadJob, Int],
      sent: Queue[SentRequest],
      received: HashMap[(PieceId, Offset), Piece],
      downSpeedAccRef: Ref[Long]
  ): RIO[Dispatcher with FileIO with Clock, Unit] = {

    handle.receive[Piece, Choke]
      .timeoutFail(new ProtocolException("Remote peer has not responded for 10 seconds"))(10.seconds)
      .flatMap {

        case resp @ Message.Piece(_, _, _) =>
          val responseKey = (resp.index, resp.offset)
          continue(
            handle,
            jobState,
            requestsPerJobs,
            sent,
            received + (responseKey -> resp),
            downSpeedAccRef
          )

        case Message.Choke                 =>
          for {
            _ <- ZIO.foreach_(requestsPerJobs.keys)(j =>
                   Dispatcher.releaseJob(handle.peerId, ReleaseJobStatus.Choked(j))
                 )
            _ <- restart(handle, DownloadState(peerChoking = true, amInterested = true), downSpeedAccRef)
          } yield ()

        case _                             => ???
      }
  }

  private def updatePeerChokingState(peerHandle: PeerHandle, oldState: DownloadState): Task[DownloadState] = {
    peerHandle.pollLast[Choke, Unchoke].map {
      case Some(Message.Choke)   => oldState.copy(peerChoking = true)
      case Some(Message.Unchoke) => oldState.copy(peerChoking = false)
      case _                     => oldState
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
