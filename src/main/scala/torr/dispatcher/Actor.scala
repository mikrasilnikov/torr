package torr.dispatcher

import torr.consoleui.ConsoleUI
import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context

import scala.collection.mutable
import scala.collection.immutable
import torr.metainfo.MetaInfo
import torr.peerwire.{PeerHandleLive, TorrBitSet}
import zio.logging.Logging
import zio.nio.core.{InetAddress, InetSocketAddress}

import java.nio.charset.StandardCharsets
import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

object Actor {

  final case class RegisteredPeer(
      address: InetSocketAddress,
      have: mutable.Set[PieceId] = mutable.HashSet[PieceId](),
      interesting: mutable.Set[PieceId] = mutable.HashSet[PieceId](),
      haveSubscriptions: ArrayBuffer[Queue[PieceId]] = ArrayBuffer[Queue[PieceId]](),
      var downloadSpeed: Int = 0,
      var uploadSpeed: Int = 0
  )

  sealed trait Command[+_]
  case class RegisterPeer(peerId: PeerId, address: InetSocketAddress)    extends Command[Unit]
  case class UnregisterPeer(peerId: PeerId)                              extends Command[Unit]
  case class PeerIsSeeding(peerId: PeerId)                               extends Command[Boolean]
  case class ReportHave(peerId: PeerId, piece: PieceId)                  extends Command[Unit]
  case class ReportHaveMany(peerId: PeerId, pieces: Set[PieceId])        extends Command[Unit]
  case class SubscribeToHaveUpdates(peerId: PeerId)                      extends Command[(Set[PieceId], Dequeue[PieceId])]
  case class AcquireJob(peerId: PeerId)                                  extends Command[AcquireJobResult]
  case class ReleaseJob(peerId: PeerId, jobWithStatus: ReleaseJobStatus) extends Command[Unit]
  case class AcquireUploadSlot(peerId: PeerId)                           extends Command[AcquireUploadSlotResult]
  case class ReleaseUploadSlot(peerId: PeerId)                           extends Command[Unit]
  case object IsDownloadCompleted                                        extends Command[DownloadCompletion]
  case class IsRemoteInteresting(peerId: PeerId)                         extends Command[Boolean]
  case class ReportDownloadSpeed(peerId: PeerId, value: Int)             extends Command[Unit]
  case class ReportUploadSpeed(peerId: PeerId, value: Int)               extends Command[Unit]
  case object NumActivePeers                                             extends Command[Int]
  case class MapState[A](f: State => A)                                  extends Command[A]

  type TimesRenewed = Int

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
      maxSimultaneousDownloads: Int = 10,
      maxSimultaneousUploads: Int = 10,
      registeredPeers: mutable.Map[PeerId, RegisteredPeer] = mutable.HashMap[PeerId, RegisteredPeer](),
      activeJobs: mutable.Map[DownloadJob, PeerId] = mutable.HashMap[DownloadJob, PeerId](),
      activePeers: mutable.Map[PeerId, ArrayBuffer[DownloadJob]] =
        mutable.HashMap[PeerId, ArrayBuffer[DownloadJob]](),
      suspendedJobs: mutable.Map[PieceId, DownloadJob] = mutable.HashMap[PieceId, DownloadJob](),
      uploadSlots: mutable.Map[PeerId, TimesRenewed] = mutable.HashMap[PeerId, TimesRenewed]()
  )

  val stateful = new Stateful[ConsoleUI with Logging, State, Command] {
    //noinspection WrapInsteadOfLiftInspection
    def receive[A](state: State, msg: Command[A], context: Context): RIO[ConsoleUI with Logging, (State, A)] =
      msg match {
        case RegisterPeer(peerId, address)      => ZIO(registerPeer(state, peerId, address)).as((state, ()))
        case UnregisterPeer(peerId)             => unregisterPeer(state, peerId).as((state, ()))
        case ReportHave(peerId, piece)          => ZIO(reportHave(state, peerId, piece)).as((state, ()))
        case ReportHaveMany(peerId, pieces)     => ZIO(reportHaveMany(state, peerId, pieces)).as((state, ()))
        case PeerIsSeeding(peerId)              => ZIO(peerIsSeeding(state, peerId)).map(res => (state, res))
        case SubscribeToHaveUpdates(peerId)     => subscribeToHaveUpdates(state, peerId).map(res => (state, res))
        case ReportDownloadSpeed(peerId, value) => ZIO(reportDownloadSpeed(state, peerId, value)).as((state, ()))
        case ReportUploadSpeed(peerId, value)   => ZIO(reportUploadSpeed(state, peerId, value)).as((state, ()))
        case AcquireJob(peerId)                 => acquireJob(state, peerId).map(res => (state, res))
        case ReleaseJob(peerId, jobWithStatus)  => releaseJob(state, peerId, jobWithStatus).as(state, ())
        case AcquireUploadSlot(peerId)          => ZIO(acquireUploadSlot(state, peerId)).map(res => (state, res))
        case ReleaseUploadSlot(peerId)          => ZIO(releaseUploadSlot(state, peerId)).as((state, ()))
        case IsDownloadCompleted                => ZIO(isDownloadCompleted(state)).map(res => (state, res))
        case IsRemoteInteresting(peerId)        => ZIO(isRemoteInteresting(state, peerId)).map(res => (state, res))
        case NumActivePeers                     => ZIO.succeed((state, state.activePeers.size))
        case MapState(f)                        => ZIO(f(state)).map(res => (state, res))
      }
  }

  private[dispatcher] def registerPeer(state: State, peerId: PeerId, address: InetSocketAddress): Unit = {
    if (state.registeredPeers.contains(peerId)) {
      val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
      throw new IllegalStateException(s"PeerId $peerIdStr is already registered")
    } else {
      state.registeredPeers.put(
        peerId,
        RegisteredPeer(
          address,
          have = mutable.HashSet[PieceId](),
          interesting = mutable.HashSet[PieceId]()
        )
      )
    }
  }

  private[dispatcher] def unregisterPeer(state: State, peerId: PeerId): RIO[Logging, Unit] = {
    val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)

    if (!state.registeredPeers.contains(peerId)) {
      ZIO.fail(new IllegalStateException(s"PeerId $peerIdStr is not registered"))

    } else {
      val peerJobs = state.activePeers.get(peerId)
      val haveSubs = state.registeredPeers(peerId).haveSubscriptions
      for {
        _ <- Logging.debug(s"$peerIdStr Dispatcher unregistering peer")
        _ <- peerJobs match {
               case Some(jobs) =>
                 ZIO.foreach_(jobs.toList)(j => releaseJob(state, peerId, ReleaseJobStatus.Aborted(j)))
               case None       => ZIO.unit
             }
        _ <- ZIO.foreach_(haveSubs)(_.shutdown)
        _  = state.registeredPeers.remove(peerId)
        _  = state.uploadSlots.remove(peerId)
      } yield ()
    }
  }

  private[dispatcher] def reportHave(state: State, peerId: PeerId, piece: PieceId): Unit = {
    if (!state.registeredPeers.contains(peerId)) {
      val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
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
      val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
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

  private[dispatcher] def peerIsSeeding(state: State, peerId: PeerId): Boolean = {
    if (!state.registeredPeers.contains(peerId)) {
      val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
      throw new IllegalStateException(s"PeerId $peerIdStr is not registered")

    } else {
      val peer = state.registeredPeers(peerId)
      peer.have.size == state.localHave.length
    }
  }

  private[dispatcher] def subscribeToHaveUpdates(
      state: State,
      peerId: PeerId
  ): Task[(Set[PieceId], Dequeue[PieceId])] = {
    if (!state.registeredPeers.contains(peerId)) {
      val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
      throw new IllegalStateException(s"Dispatcher.subscribeToHaveUpdates PeerId $peerIdStr is not registered")
    } else {
      val localHave = state.localHave
      val initial   = immutable.HashSet.from(localHave.indices.filter(localHave))
      for {
        queue <- Queue.unbounded[PieceId]
        _      = state.registeredPeers(peerId).haveSubscriptions.append(queue)
      } yield (initial, queue)
    }
  }

  private def reportDownloadSpeed(state: State, peerId: PeerId, value: Int): Unit = {
    state.registeredPeers.get(peerId) match {
      case Some(peer) => peer.downloadSpeed = value
      case None       =>
        val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
        throw new IllegalStateException(s"PeerId $peerIdStr is not registered")
    }
  }

  private def reportUploadSpeed(state: State, peerId: PeerId, value: Int): Unit = {
    state.registeredPeers.get(peerId) match {
      case Some(peer) => peer.uploadSpeed = value
      case None       =>
        val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
        throw new IllegalStateException(s"PeerId $peerIdStr is not registered")
    }
  }

  private[dispatcher] def acquireJob(
      state: State,
      peerId: PeerId
  ): Task[AcquireJobResult] = {

    if (!state.registeredPeers.contains(peerId)) {
      val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
      ZIO.fail(new IllegalStateException(s"Peer $peerIdStr is not registered"))

    } else {
      tryGetSuitableJob(state, peerId) match {
        case None                                                                 =>
          ZIO.succeed(AcquireJobResult.NoInterestingPieces)

        // If peer is active (currently downloading) we must provide it with next job immediately
        // to avoid draining of request queue.
        case Some(job) if state.activePeers.contains(peerId)                      =>
          state.activeJobs.put(job, peerId)
          state.activePeers(peerId).append(job)
          ZIO.succeed(AcquireJobResult.Success(job))

        case Some(job) if state.activePeers.size < state.maxSimultaneousDownloads =>
          state.activeJobs.put(job, peerId)
          state.activePeers.put(peerId, ArrayBuffer[DownloadJob](job))
          ZIO.succeed(AcquireJobResult.Success(job))

        case _                                                                    =>
          ZIO.succeed(AcquireJobResult.OnQueue)
      }
    }
  }

  private[dispatcher] def releaseJob(state: State, peerId: PeerId, releaseStatus: ReleaseJobStatus): Task[Unit] = {
    val peerIdStr      = PeerHandleLive.makePeerIdStr(peerId)
    val peerJobsOption = state.activePeers.get(peerId)

    if (!state.registeredPeers.contains(peerId)) {
      ZIO.fail(new IllegalStateException(s"PeerId $peerIdStr is not registered"))

    } else if (peerJobsOption.isEmpty) {
      ZIO.fail(new IllegalStateException(s"Peer $peerIdStr is releasing job $releaseStatus while not being active"))

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
          case ReleaseJobStatus.Downloaded(_) =>
            for {
              _ <- handleReleasedJob(state, releaseStatus)
              _  = peerJobs.remove(peerJobIndex)
            } yield ()

          case _                              =>
            for {
              _ <- handleReleasedJob(state, releaseStatus)
              _  = peerJobs.remove(peerJobIndex)
              _  = if (peerJobs.isEmpty) state.activePeers.remove(peerId)
            } yield ()
        }
      }
    }
  }

  private def handleReleasedJob(state: State, releaseStatus: ReleaseJobStatus): Task[Unit] = {
    import ReleaseJobStatus._, JobCompletionStatus._

    val job = releaseStatus.job
    state.activeJobs.remove(job)

    val completionStatus = job.completionStatus

    (releaseStatus, completionStatus) match {

      case (Downloaded(job), Verified) if !state.localHave(job.pieceId) =>
        val updateQueues = state.registeredPeers.flatMap { case (_, p) => p.haveSubscriptions }
        for {
          _ <- ZIO.foreach_(updateQueues)(_.offer(job.pieceId))
          _  = state.localHave(job.pieceId) = true
          _  = state.registeredPeers.foreach { case (_, peer) => peer.interesting.remove(job.pieceId) }
        } yield ()

      case (Downloaded(job), Verified) if state.localHave(job.pieceId)  =>
        ZIO.fail(new IllegalStateException(
          s"$job has been released in completed state while piece ${job.pieceId} has been downloaded previously"
        ))

      case (Choked(job), Incomplete)                                    =>
        ZIO.succeed {
          state.suspendedJobs.put(job.pieceId, job)
        }

      case (Aborted(job), Incomplete)                                   =>
        ZIO.succeed {
          state.suspendedJobs.put(job.pieceId, job)
        }

      case (_, Failed)                                                  => ZIO.unit

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

    if (suspendedJobOption.isDefined) {
      val job = suspendedJobOption.get
      state.suspendedJobs.remove(job.pieceId)
      Some(job)

    } else {
      // Piece that has not been downloaded and is not being downloaded currently.
      val pieceIdOption = availableFromPeer
        .find(pid => !state.activeJobs.exists { case (job, _) => pid == job.pieceId })

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
  }

  private def acquireUploadSlot(state: State, peerId: PeerId): AcquireUploadSlotResult = {
    if (!state.registeredPeers.contains(peerId)) {
      val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)
      throw new IllegalStateException(s"Dispatcher.acquireUploadSlot PeerId $peerIdStr is not registered")

    } else {
      if (state.uploadSlots.size <= state.maxSimultaneousDownloads) {
        state.uploadSlots.get(peerId) match {
          case Some(times) => state.uploadSlots.update(peerId, times + 1)
          case None        => state.uploadSlots.put(peerId, 0)
        }
        AcquireUploadSlotResult.Granted
      } else {
        val maxTimesRenewed = state.uploadSlots.maxBy { case (_, times) => times }._2
        state.uploadSlots.get(peerId) match {
          case Some(times) if times == maxTimesRenewed =>
            AcquireUploadSlotResult.Denied
          case Some(times)                             =>
            state.uploadSlots.update(peerId, times + 1)
            AcquireUploadSlotResult.Granted
          case None                                    =>
            AcquireUploadSlotResult.Denied
        }
      }
    }
  }

  private def releaseUploadSlot(state: State, peerId: PeerId): Unit = {
    val peerIdStr = PeerHandleLive.makePeerIdStr(peerId)

    if (!state.registeredPeers.contains(peerId)) {
      throw new IllegalStateException(s"Dispatcher.releaseUploadSlot PeerId $peerIdStr is not registered")

    } else if (!state.uploadSlots.contains(peerId)) {
      throw new IllegalStateException(s"Dispatcher.releaseUploadSlot PeerId $peerIdStr has not acquired an upload slot")

    } else {
      state.uploadSlots.remove(peerId)
    }
  }

  private[dispatcher] def isDownloadCompleted(state: State): DownloadCompletion = {

    @tailrec
    def loop(i: Int, haveAll: Boolean): DownloadCompletion = {
      if (i >= state.localHave.length && haveAll)
        DownloadCompletion.Completed
      else if (i >= state.localHave.length)
        DownloadCompletion.EndGame
      else if (state.localHave(i))
        loop(i + 1, haveAll)
      else if (state.activeJobs.exists { case (job, _) => job.pieceId == i })
        loop(i + 1, haveAll = false)
      else DownloadCompletion.InProgress
    }

    loop(0, haveAll = true)
  }

  private[dispatcher] def isRemoteInteresting(state: State, peerId: PeerId): Boolean = {
    state.registeredPeers(peerId).interesting.nonEmpty
  }
}
