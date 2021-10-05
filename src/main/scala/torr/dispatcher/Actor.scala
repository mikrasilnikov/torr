package torr.dispatcher

import torr.consoleui.ConsoleUI
import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context

import scala.collection.mutable
import torr.metainfo.MetaInfo

import java.nio.charset.StandardCharsets
import scala.collection.mutable.ArrayBuffer

object Actor {

  final case class RegisteredPeer(
      have: mutable.Set[PieceId] = mutable.HashSet[PieceId](),
      interesting: mutable.Set[PieceId] = mutable.HashSet[PieceId](),
      var downloadSpeed: Int = 0,
      var uploadSpeed: Int = 0
  )

  sealed trait Command[+_]
  case class RegisterPeer(peerId: PeerId)                                              extends Command[Unit]
  case class UnregisterPeer(peerId: PeerId)                                            extends Command[Unit]
  case class ReportHaveMany(peerId: PeerId, pieces: Set[PieceId])                      extends Command[Unit]
  case class ReportHave(peerId: PeerId, piece: PieceId)                                extends Command[Unit]
  case class AcquireJob(peerId: PeerId, promise: Promise[Throwable, AcquireJobResult]) extends Command[Unit]
  case class ReleaseJob(peerId: PeerId, jobWithStatus: ReleaseJobStatus)               extends Command[Unit]
  case object IsDownloadCompleted                                                      extends Command[Boolean]
  case class IsRemoteInteresting(peerId: PeerId)                                       extends Command[Boolean]
  case class ReportDownloadSpeed(peerId: PeerId, value: Int)                           extends Command[Unit]
  case class ReportUploadSpeed(peerId: PeerId, value: Int)                             extends Command[Unit]

  private[dispatcher] case object DrawProgress extends Command[Unit]

  /**
    * Code that is responsible for interacting with a remote peer (a `peer routine`) is expected to allocate,
    * execute and release download jobs. The dispatcher service keeps track of such jobs.
    * A job may be returned to the dispatcher in one of two states:
    *  - completed state (all blocks have been successfully downloaded)
    *  - failed state (in case of `choking` or protocol error)
    *  A failed job is considered `suspended` and is handed to the first suitable `peer routine` to carry on
    *  working on it.
    */
  case class State(
      metaInfo: MetaInfo,
      localHave: Array[Boolean],
      maxActivePeers: Int = 5, // Number of simultaneous downloads.
      registeredPeers: mutable.Map[PeerId, RegisteredPeer] = mutable.HashMap[PeerId, RegisteredPeer](),
      activeJobs: mutable.Map[DownloadJob, PeerId] = mutable.HashMap[DownloadJob, PeerId](),
      activePeers: mutable.Map[PeerId, ArrayBuffer[DownloadJob]] =
        mutable.HashMap[PeerId, ArrayBuffer[DownloadJob]](),
      pendingJobRequests: mutable.Map[PeerId, Promise[Throwable, AcquireJobResult]] =
        mutable.HashMap[PeerId, Promise[Throwable, AcquireJobResult]](),
      suspendedJobs: mutable.Map[PieceId, DownloadJob] = mutable.HashMap[PieceId, DownloadJob]()
  )

  val stateful = new Stateful[ConsoleUI, State, Command] {
    //noinspection WrapInsteadOfLiftInspection
    def receive[A](state: State, msg: Command[A], context: Context): RIO[ConsoleUI, (State, A)] =
      msg match {
        case RegisterPeer(peerId)               => ZIO(registerPeer(state, peerId)).as((state, ()))
        case UnregisterPeer(peerId)             => ZIO(unregisterPeer(state, peerId)).as((state, ()))
        case ReportHave(peerId, piece)          => ZIO(reportHave(state, peerId, piece)).as((state, ()))
        case ReportHaveMany(peerId, pieces)     => ZIO(reportHaveMany(state, peerId, pieces)).as((state, ()))
        case ReportDownloadSpeed(peerId, value) => ZIO(reportDownloadSpeed(state, peerId, value)).as((state, ()))
        case ReportUploadSpeed(peerId, value)   => ZIO(reportUploadSpeed(state, peerId, value)).as((state, ()))
        case AcquireJob(peerId, promise)        => acquireJob(state, peerId, promise).as((state, ()))
        case ReleaseJob(peerId, jobWithStatus)  => releaseJob(state, peerId, jobWithStatus).as(state, ())
        case IsDownloadCompleted                => ZIO(isDownloadCompleted(state)).map(res => (state, res))
        case IsRemoteInteresting(peerId)        => ZIO(isRemoteInteresting(state, peerId)).map(res => (state, res))
        case DrawProgress                       => drawProgress(state).as(state, ())
      }
  }

  private[dispatcher] def registerPeer(state: State, peerId: PeerId): Unit = {
    if (state.registeredPeers.contains(peerId)) {
      val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)
      throw new IllegalStateException(s"PeerId $peerIdStr is already registered")
    } else {
      state.registeredPeers.put(
        peerId,
        RegisteredPeer(
          have = mutable.HashSet[PieceId](),
          interesting = mutable.HashSet[PieceId]()
        )
      )
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

  private[dispatcher] def reportHave(state: State, peerId: PeerId, piece: PieceId): Unit = {
    if (!state.registeredPeers.contains(peerId)) {
      val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)
      throw new IllegalStateException(s"PeerId $peerIdStr is not registered")
    } else {
      val peer = state.registeredPeers(peerId)
      peer.have.add(piece)
      if (!state.localHave(piece))
        peer.interesting.add(piece)
    }
  }

  private[dispatcher] def reportHaveMany(state: State, peerId: PeerId, pieces: Set[PieceId]): Unit = {
    if (!state.registeredPeers.contains(peerId)) {
      val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)
      throw new IllegalStateException(s"PeerId $peerIdStr is not registered")
    } else {
      pieces.foreach { piece =>
        val peer = state.registeredPeers(peerId)
        peer.have.add(piece)
        if (!state.localHave(piece))
          peer.interesting.add(piece)
      }
    }
  }

  private def reportDownloadSpeed(state: State, peerId: PeerId, value: Int): Unit = {
    state.registeredPeers.get(peerId) match {
      case Some(peer) => peer.downloadSpeed = value
      case None       =>
        val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)
        throw new IllegalStateException(s"PeerId $peerIdStr is not registered")
    }
  }

  private def reportUploadSpeed(state: State, peerId: PeerId, value: Int): Unit = {
    state.registeredPeers.get(peerId) match {
      case Some(peer) => peer.uploadSpeed = value
      case None       =>
        val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)
        throw new IllegalStateException(s"PeerId $peerIdStr is not registered")
    }
  }

  private[dispatcher] def acquireJob(
      state: State,
      peerId: PeerId,
      promise: Promise[Throwable, AcquireJobResult]
  ): Task[Unit] = {
    val peerIdStr = new String(peerId.toArray, StandardCharsets.US_ASCII)

    if (!state.registeredPeers.contains(peerId)) {
      promise.fail(new IllegalStateException(s"Peer $peerIdStr is not registered")).unit

    } else if (state.pendingJobRequests.contains(peerId)) {
      promise.fail(new IllegalStateException(s"Peer $peerIdStr is already waiting for a job")).unit

    } else {
      val suitableJobOption = tryGetSuitableJob(state, peerId)
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
          state.registeredPeers.foreach { case (_, peer) => peer.interesting.remove(job.pieceId) }
          promise.succeed(AcquireJobResult.AcquireSuccess(job)).unit

        } else if (state.activePeers.size < state.maxActivePeers) {
          state.activeJobs.put(job, peerId)
          state.activePeers.put(peerId, ArrayBuffer[DownloadJob](job))
          state.registeredPeers.foreach { case (_, peer) => peer.interesting.remove(job.pieceId) }
          promise.succeed(AcquireJobResult.AcquireSuccess(job)).unit

        } else {
          state.pendingJobRequests.put(peerId, promise)
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

    } else if (state.pendingJobRequests.contains(peerId)) {
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
  private def handlePendingJobRequests(state: State, jobsToAcquire: Int = 1): Task[Unit] = {

    def loop(peers: List[PeerId], jobsToAcquire: Int): Task[Unit] = {
      peers match {
        case Nil         => ZIO.unit
        case pid :: tail =>
          val promise           = state.pendingJobRequests(pid)
          val suitableJobOption = tryGetSuitableJob(state, pid)

          suitableJobOption match {
            case Some(job) if jobsToAcquire > 0 =>
              state.activeJobs.put(job, pid)
              state.activePeers.put(pid, ArrayBuffer(job))
              state.pendingJobRequests.remove(pid)

              state.registeredPeers.foreach { case (_, peer) => peer.interesting.remove(job.pieceId) }
              promise.succeed(AcquireJobResult.AcquireSuccess(job)) *>
                loop(tail, jobsToAcquire - 1)

            case Some(_)                        =>
              loop(tail, jobsToAcquire)

            case None                           =>
              state.pendingJobRequests.remove(pid)
              promise.succeed(AcquireJobResult.NotInterested) *>
                loop(tail, jobsToAcquire)
          }
      }
    }

    loop(state.pendingJobRequests.keys.toList, jobsToAcquire)
  }

  private def handleReleasedJob(state: State, releaseStatus: ReleaseJobStatus): Task[Unit] = {
    import ReleaseJobStatus._, JobCompletionStatus._

    val job = releaseStatus.job
    state.activeJobs.remove(job)

    val completionStatus = job.completionStatus

    (releaseStatus, completionStatus) match {

      case (Active(job), Complete)   =>
        ZIO.succeed {
          state.localHave(job.pieceId) = true

          state.registeredPeers.foreach {
            case (_, peer) =>
              peer.interesting.remove(job.pieceId)
          }
        }

      case (Choked(job), Incomplete) =>
        ZIO.succeed {
          state.suspendedJobs.put(job.pieceId, job)

          state.registeredPeers.foreach {
            case (_, peer) =>
              if (peer.have.contains(job.pieceId))
                peer.interesting.add(job.pieceId)
          }
        }

      case (_, Failed)               => ZIO.unit

      case _ =>
        ZIO.fail(new IllegalStateException(
          s"ReleaseJobStatus status does not correspond to JobCompletionStatus: ($releaseStatus, ${job.completionStatus})"
        ))
    }
  }

  private def tryGetSuitableJob(state: State, peerId: PeerId): Option[DownloadJob] = {

    val peer              = state.registeredPeers(peerId)
    val availableFromPeer = peer.interesting

    val suspendedJobOption = state.suspendedJobs.view
      .find { case (piece, _) => availableFromPeer.contains(piece) }
      .map { case (_, job) => job }

    def newJobOption: Option[DownloadJob] = {
      // Piece that has not been downloaded and is not being downloaded currently.
      val pieceIdOption = availableFromPeer.headOption

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
    state.localHave.forall(identity)
  }

  private[dispatcher] def isRemoteInteresting(state: State, peerId: PeerId): Boolean = {
    state.registeredPeers(peerId).interesting.nonEmpty
  }

  private def drawProgress(state: State): RIO[ConsoleUI, Unit] = {
    for {
      _ <- ConsoleUI.clear
      _ <- ConsoleUI.draw(state)
    } yield ()
  }
}
