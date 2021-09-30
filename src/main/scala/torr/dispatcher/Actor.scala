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
      maxActivePeers: Int = 5, // Number of simultaneous downloads.
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
      promise.fail(new IllegalStateException(s"Peer $peerIdStr is not registered")).unit

    } else if (state.pendingJobRequests.exists(req => req.peerId == peerId)) {
      promise.fail(new IllegalStateException(s"Peer $peerIdStr is already waiting for a job")).unit

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

        } else if (state.activePeers.size < state.maxActivePeers) {
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

    } else if (state.pendingJobRequests.exists(req => req.peerId == peerId)) {
      ZIO.fail(new IllegalStateException(s"Active peer $peerIdStr can not have a pending job request"))

    } else {
      val peerJobs = peerJobsOption.get
      if (
        !state.activeJobs.contains(releaseStatus.job) ||
        !peerJobs.contains(releaseStatus.job)
      ) {
        ZIO.fail(new IllegalStateException(
          s"Peer $peerIdStr is releasing job $releaseStatus that has not been acquired"
        ))

      } else {
        val job          = releaseStatus.job
        val peerJobIndex = peerJobs.indexOf(job)

        releaseStatus match {
          case ReleaseJobStatus.Active(_) =>
            for {
              _ <- handleReleasedJob(state, releaseStatus)
              _  = peerJobs.remove(peerJobIndex)
              _ <- handlePendingJobRequests(
                     state,
                     jobsToAcquire = state.maxActivePeers - state.activePeers.size
                   )
            } yield ()

          case ReleaseJobStatus.Choked(_) =>
            if (peerJobs.length > 1) {
              ZIO.fail(new IllegalStateException(
                s"Peer $peerIdStr is releasing $releaseStatus while having multiple jobs allocated: ${peerJobs.mkString(",")}"
              ))
            } else {
              for {
                _ <- handleReleasedJob(state, releaseStatus)
                _  = peerJobs.remove(peerJobIndex)
                _  = state.activePeers.remove(peerId)
                _ <- handlePendingJobRequests(
                       state,
                       jobsToAcquire = state.maxActivePeers - state.activePeers.size
                     )
              } yield ()
            }
        }
      }
    }
  }

  /**
    * Checks every pending job request if it is still must wait for a response.
    *   - may acquire at most `jobsToAcquire` jobs (and fulfill corresponding requests)
    *   - responds to everyone if download is completed
    *   - aborts requests to peers that are not interesting anymore
    */
  //noinspection SimplifyUnlessInspection
  private def handlePendingJobRequests(state: State, jobsToAcquire: Int = 1, i: Int = 0): Task[Unit] = {

    if (i >= state.pendingJobRequests.length) ZIO.unit
    else {
      val pendingRequest    = state.pendingJobRequests(i)
      val downloadCompleted = isDownloadCompleted(state)
      val suitableJobOption = tryGetSuitableJob(state, pendingRequest.remoteHave)

      suitableJobOption match {
        case Some(job) if jobsToAcquire > 0 =>
          state.activeJobs.put(job, pendingRequest.peerId)
          state.activePeers.put(pendingRequest.peerId, ArrayBuffer(job))
          state.pendingJobRequests.remove(i)

          pendingRequest.promise.succeed(AcquireJobResult.AcquireSuccess(job)) *>
            handlePendingJobRequests(state, jobsToAcquire - 1, i)

        case Some(_)                        =>
          handlePendingJobRequests(state, jobsToAcquire, i + 1)

        case None if downloadCompleted      =>
          state.pendingJobRequests.remove(i)
          pendingRequest.promise.succeed(AcquireJobResult.DownloadCompleted) *>
            handlePendingJobRequests(state, jobsToAcquire, i)

        case None                           =>
          state.pendingJobRequests.remove(i)
          pendingRequest.promise.succeed(AcquireJobResult.NotInterested) *>
            handlePendingJobRequests(state, jobsToAcquire, i)
      }
    }
  }

  private def handleReleasedJob(state: State, releaseStatus: ReleaseJobStatus): Task[Unit] = {
    val job = releaseStatus.job
    state.activeJobs.remove(job)

    import ReleaseJobStatus._, JobCompletionStatus._

    val completionStatus = job.completionStatus

    (releaseStatus, completionStatus) match {

      case (Active(job), Complete)   =>
        ZIO.succeed(state.localHave(job.pieceId) = true)

      case (Choked(job), Incomplete) =>
        ZIO.succeed(state.suspendedJobs.put(job.pieceId, job))

      case (_, Failed)               => ZIO.unit

      case _ =>
        ZIO.fail(new IllegalStateException(
          s"ReleaseJobStatus status does not correspond to JobCompletionStatus: ($releaseStatus, ${job.completionStatus})"
        ))
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
