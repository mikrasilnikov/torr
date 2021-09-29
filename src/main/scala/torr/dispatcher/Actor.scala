package torr.dispatcher

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import scala.collection.mutable
import torr.metainfo.MetaInfo
import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

object Actor {
  sealed trait Command[+_]
  case class RegisterPeer(peerId: PeerId)                                extends Command[Unit]
  case class UnregisterPeer(peerId: PeerId)                              extends Command[Unit]
  case class AcquireJob(peerId: PeerId, remoteHave: Set[PieceId], promise: Promise[Throwable, AcquireJobResult])
      extends Command[Unit]
  case class ReleaseJob(peerId: PeerId, jobWithStatus: ReleaseJobStatus) extends Command[Unit]
  case object IsDownloadCompleted                                        extends Command[Boolean]
  case class IsRemoteInteresting(remoteHave: Set[PieceId])               extends Command[Boolean]

  final case class AcquireJobRequest(
      peerId: PeerId,
      remoteHave: Set[PieceId],
      promise: Promise[Throwable, AcquireJobResult]
  )

  /**
    * Code that is responsible for interacting with a remote peer (a `peer procedure`) is expected to allocate,
    * execute and release download jobs. The dispatcher service keeps track of such jobs.
    * A job may be returned to the dispatcher in one of two states:
    *  - completed state (all blocks have been successfully downloaded)
    *  - failed state (in case of `choking` or protocol error)
    *  A failed job is considered `suspended` and is handed to the first suitable `peer procedure` to carry on
    *  working on it.
    */
  case class State(
      metaInfo: MetaInfo,
      localHave: Array[Boolean],
      maxConcurrentDownloads: Int = 5,
      registeredPeers: mutable.Set[PeerId] = new mutable.HashSet[PeerId](),
      activeJobs: mutable.Map[DownloadJob, PeerId] = new mutable.HashMap[DownloadJob, PeerId](),
      activePeers: mutable.Map[PeerId, ArrayBuffer[DownloadJob]] =
        new mutable.HashMap[PeerId, ArrayBuffer[DownloadJob]](),
      pendingJobRequests: ArrayBuffer[AcquireJobRequest] = ArrayBuffer[AcquireJobRequest](),
      suspendedJobs: mutable.Map[PieceId, DownloadJob] = new mutable.HashMap[PieceId, DownloadJob]()
  )

  val stateful = new Stateful[Any, State, Command] {
    //noinspection WrapInsteadOfLiftInspection
    def receive[A](state: State, msg: Command[A], context: Context): RIO[Any, (State, A)] =
      msg match {
        case RegisterPeer(peerId)              => ZIO(registerPeer(state, peerId)).map(res => (state, res))
        case UnregisterPeer(peerId)            => ZIO(unregisterPeer(state, peerId)).map(res => (state, res))
        case AcquireJob(peerId, have, promise) => acquireJob(state, peerId, have, promise).as((state, ()))
        case ReleaseJob(peerId, jobWithStatus) => releaseJob(state, peerId, jobWithStatus).as(state, ())
        case IsDownloadCompleted               => ZIO(isDownloadCompleted(state)).map(res => (state, res))
        case IsRemoteInteresting(have)         => ZIO(isRemoteInteresting(state, have)).map(res => (state, res))
      }
  }

  private[dispatcher] def registerPeer(state: State, peerId: PeerId): Unit = {
    if (state.registeredPeers.contains(peerId)) {
      val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)
      throw new IllegalStateException(s"PeerId $peerIdStr is already registered")
    } else {
      state.registeredPeers.add(peerId)
    }
  }

  private[dispatcher] def unregisterPeer(state: State, peerId: PeerId): Unit = {
    if (!state.registeredPeers.contains(peerId)) {
      val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)
      throw new IllegalStateException(s"PeerId $peerIdStr is not registered")
    } else {

      val peerJobs = state.activePeers.get(peerId)
      peerJobs match {
        case None       => ()
        case Some(jobs) =>
          jobs.foreach(state.activeJobs.remove)
          state.activePeers.remove(peerId)
      }

      state.registeredPeers.remove(peerId)
    }
  }

  private[dispatcher] def acquireJob(
      state: State,
      peerId: PeerId,
      remoteHave: Set[PieceId],
      promise: Promise[Throwable, AcquireJobResult]
  ): Task[Unit] = {
    val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)

    if (!state.registeredPeers.contains(peerId)) {
      throw new IllegalStateException(s"Peer $peerIdStr is not registered")

    } else if (state.pendingJobRequests.exists(req => req.peerId == peerId)) {
      ZIO.fail(new IllegalStateException(s"Peer $peerIdStr is already waiting for a job"))

    } else if (isDownloadCompleted(state)) {
      promise.succeed(AcquireJobResult.DownloadCompleted).unit

    } else {
      val suitableJobOption = tryGetSuitableJob(state, remoteHave)
      if (suitableJobOption.isEmpty) {
        promise.succeed(AcquireJobResult.NotInterested).unit

      } else {
        val job = suitableJobOption.get

        // If a peer is currently active uploading to us, we should satisfy job request immediately
        // to avoid draining of the block request queue.
        val peerIsActive = state.activePeers.contains(peerId)
        if (peerIsActive) {
          state.activeJobs.put(job, peerId)
          state.activePeers(peerId).append(job)
          promise.succeed(AcquireJobResult.AcquireSuccess(job)).unit

        } else if (state.activePeers.size < state.maxConcurrentDownloads) {
          state.activeJobs.put(job, peerId)
          state.activePeers.put(peerId, ArrayBuffer[DownloadJob](job))
          promise.succeed(AcquireJobResult.AcquireSuccess(job)).unit

        } else {
          state.pendingJobRequests.append(AcquireJobRequest(peerId, remoteHave, promise))
          ZIO.unit
        }
      }
    }
  }

  private[dispatcher] def releaseJob(state: State, peerId: PeerId, releaseStatus: ReleaseJobStatus): Task[Unit] = {
    val peerIdStr      = new String(peerId.toArray, StandardCharsets.US_ASCII)
    val peerJobsOption = state.activePeers.get(peerId)

    if (!state.registeredPeers.contains(peerId)) {
      ZIO.fail(new IllegalStateException(s"PeerId $peerIdStr is not registered"))

    } else if (peerJobsOption.isEmpty) {
      ZIO.fail(new IllegalStateException(s"Peer $peerIdStr is releasing job $releaseStatus while not being active"))

    } else {
      val peerJobs = peerJobsOption.get
      if (!peerJobs.contains(releaseStatus.job)) {
        ZIO.fail(new IllegalStateException(
          s"Peer $peerIdStr is releasing job $releaseStatus that has not been acquired"
        ))

      } else {
        val job          = releaseStatus.job
        val peerJobIndex = peerJobs.indexOf(job)

        handleReleasedJob(state, releaseStatus)

        releaseStatus match {
          case ReleaseJobStatus.Downloading(_) =>
            peerJobs.remove(peerJobIndex)

          case ReleaseJobStatus.Choked(_)      =>
            if (peerJobs.length > 1) {
              throw new IllegalStateException(
                s"Peer $peerIdStr is releasing $releaseStatus while having multiple jobs allocated: ${peerJobs.mkString(",")}"
              )
            } else {
              peerJobs.remove(peerJobIndex)
              state.activePeers.remove(peerId)
            }
        }

        processPendingJobRequests(state)
      }
    }
  }

  //noinspection SimplifyUnlessInspection
  private def processPendingJobRequests(state: State, i: Int = 0, fulfilled: Boolean = false): Task[Unit] = {

    if (i >= state.pendingJobRequests.length) ZIO.unit
    else {
      val pendingRequest = state.pendingJobRequests(i)

      // A peer can not be active and have a pending request at the same time.
      // All incoming job requests for active peers must be fulfilled immediately.
      if (
        state.activePeers.contains(pendingRequest.peerId) ||
        state.activeJobs.exists { case (_, pid) => pid == pendingRequest.peerId }
      ) {
        val peerIdStr = new String(pendingRequest.peerId.toArray, StandardCharsets.US_ASCII)
        ZIO.fail(s"Active peer $peerIdStr can not have a pending job request")
      }

      val downloadCompleted = isDownloadCompleted(state)
      val suitableJobOption = tryGetSuitableJob(state, pendingRequest.remoteHave)

      suitableJobOption match {
        case Some(job) if !fulfilled   =>
          state.activeJobs.put(job, pendingRequest.peerId)
          state.activePeers.put(pendingRequest.peerId, ArrayBuffer(job))
          state.pendingJobRequests.remove(i)

          pendingRequest.promise.succeed(AcquireJobResult.AcquireSuccess(job)) *>
            processPendingJobRequests(state, i, fulfilled = true)

        case Some(_)                   =>
          processPendingJobRequests(state, i + 1, fulfilled)

        case None if downloadCompleted =>
          pendingRequest.promise.succeed(AcquireJobResult.DownloadCompleted) *>
            processPendingJobRequests(state, i + 1, fulfilled)

        case None                      =>
          pendingRequest.promise.succeed(AcquireJobResult.NotInterested) *>
            processPendingJobRequests(state, i + 1, fulfilled)
      }
    }
  }

  private def handleReleasedJob(state: State, releaseStatus: ReleaseJobStatus): Unit = {
    val job = releaseStatus.job
    state.activeJobs.remove(job)
    job.completionStatus match {
      case JobCompletionStatus.Complete   => state.localHave(job.pieceId) = true
      case JobCompletionStatus.Incomplete => state.suspendedJobs.put(job.pieceId, job)
      case JobCompletionStatus.Failed     => ()
    }
  }

  private def tryGetSuitableJob(state: State, remoteHave: Set[PieceId]): Option[DownloadJob] = {

    val suspendedJobOption = state.suspendedJobs.view
      .find { case (id, _) => remoteHave.contains(id) }
      .map { case (_, job) => job }

    def newJobOption: Option[DownloadJob] = {
      // Piece that has not been downloaded and is not being downloaded currently.
      val pieceIdOption = remoteHave.find(rid =>
        !state.activeJobs.keys.exists(j => j.pieceId == rid) &&
          !state.localHave(rid)
      )

      pieceIdOption.flatMap { pid =>
        val pieceSize =
          if (pid == state.metaInfo.pieceHashes.size - 1)
            state.metaInfo.torrentSize - pid * state.metaInfo.pieceSize
          else
            state.metaInfo.pieceSize

        val job = DownloadJob(pid, pieceSize.toInt, state.metaInfo.pieceHashes(pid))
        Some(job)
      }
    }

    suspendedJobOption.orElse(newJobOption)
  }

  private[dispatcher] def isDownloadCompleted(state: State): Boolean = {
    state.metaInfo.pieceHashes.indices.forall(state.localHave)
  }

  private[dispatcher] def isRemoteInteresting(state: State, remoteHave: Set[PieceId]): Boolean = {
    state.metaInfo.pieceHashes.indices
      .exists(pieceId => remoteHave(pieceId) && !state.localHave(pieceId))
  }
}
