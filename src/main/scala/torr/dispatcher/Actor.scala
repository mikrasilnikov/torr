package torr.dispatcher

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import scala.collection.mutable
import torr.metainfo.MetaInfo
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

object Actor {
  sealed trait Command[+_]
  case class RegisterPeer(peerId: PeerId)                         extends Command[Unit]
  case class UnregisterPeer(peerId: PeerId)                       extends Command[Unit]
  case class AcquireJob(peerId: PeerId, remoteHave: Set[PieceId]) extends Command[AcquireJobResult]
  case class ReleaseJob(peerId: PeerId, job: DownloadJob)         extends Command[Unit]
  case object IsDownloadCompleted                                 extends Command[Boolean]
  case class IsRemoteInteresting(remoteHave: Set[PieceId])        extends Command[Boolean]

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
      registeredPeers: mutable.Set[PeerId] = new mutable.HashSet[PeerId](),
      // A peer can have only one job assigned to.
      activeJobs: mutable.Map[(PieceId, PeerId), DownloadJob] = new mutable.HashMap[(PieceId, PeerId), DownloadJob](),
      suspendedJobs: mutable.Map[PieceId, DownloadJob] = new mutable.HashMap[PieceId, DownloadJob]()
  )

  val stateful = new Stateful[Any, State, Command] {
    //noinspection WrapInsteadOfLiftInspection
    def receive[A](state: State, msg: Command[A], context: Context): RIO[Any, (State, A)] =
      msg match {
        case RegisterPeer(peerId)      => ZIO(registerPeer(state, peerId)).map(res => (state, res))
        case UnregisterPeer(peerId)    => ZIO(unregisterPeer(state, peerId)).map(res => (state, res))
        case AcquireJob(peerId, have)  => ZIO(acquireJob(state, peerId, have)).map(res => (state, res))
        case ReleaseJob(peerId, job)   => ZIO(releaseJob(state, peerId, job)).as(state, ())
        case IsDownloadCompleted       => ZIO(isDownloadCompleted(state)).map(res => (state, res))
        case IsRemoteInteresting(have) => ZIO(isRemoteInteresting(state, have)).map(res => (state, res))
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

      val assignedJobOption = state.activeJobs
        .find { case ((_, pid), _) => pid == peerId }
        .map { case ((_, _), job) => job }

      assignedJobOption.fold(())(releaseJob(state, peerId, _))
      state.registeredPeers.remove(peerId)
    }
  }

  private[dispatcher] def acquireJob(state: State, peerId: PeerId, remoteHave: Set[PieceId]): AcquireJobResult = {

    // There is no problem having linear time complexity for this check because large torrents have ~1000 pieces.
    val isDownloadCompleted = state.metaInfo.pieceHashes.indices.forall(i => state.localHave(i))

    if (isDownloadCompleted) { DownloadCompleted }
    else {
      val suspended = state.suspendedJobs.view
        .filterKeys(id => remoteHave.contains(id))
        .headOption

      suspended match {
        case Some((_, job)) => AcquireSuccess(job)
        case None           =>
          val idOption = remoteHave.find(remoteId =>
            !state.activeJobs.keySet.exists { case (pieceId, _) => pieceId == remoteId } &&
              !state.localHave(remoteId)
          )

          idOption.fold[AcquireJobResult](NotInterested) { pieceId =>
            val pieceSize =
              if (pieceId == state.metaInfo.pieceHashes.size - 1)
                state.metaInfo.torrentSize - pieceId * state.metaInfo.pieceSize
              else
                state.metaInfo.pieceSize

            val job = DownloadJob(pieceId, pieceSize.toInt)
            state.activeJobs.put((job.pieceId, peerId), job)
            AcquireSuccess(job)
          }
      }
    }
  }

  private[dispatcher] def releaseJob(state: State, peerId: PeerId, job: DownloadJob): Unit = {

    val jobKey = (job.pieceId, peerId)

    if (!state.activeJobs.keySet.contains(jobKey)) {
      throw new IllegalArgumentException(s"$job is not in state.activeJobs")

    } else if (job.isCompleted) {
      val expected = state.metaInfo.pieceHashes(job.pieceId)
      val actual   = Chunk.fromArray(job.digest.clone().asInstanceOf[MessageDigest].digest())

      if (expected == actual) {
        state.activeJobs.remove(jobKey)
        state.localHave(job.pieceId) = true

      } else {
        // Piece hash is not equal to the expected metainfo value.
        // We must re-download piece from another peer.
        state.activeJobs.remove(jobKey)
        throw new IllegalStateException(s"Piece hash for piece ${job.pieceId} is not correct")
      }
    } else {
      state.activeJobs.remove(jobKey)
      state.suspendedJobs.put(job.pieceId, job)
    }
  }

  private[dispatcher] def isDownloadCompleted(state: State): Boolean = {
    state.metaInfo.pieceHashes.indices.forall(state.localHave)
  }

  private[dispatcher] def isRemoteInteresting(state: State, remoteHave: Set[PieceId]): Boolean = {
    state.metaInfo.pieceHashes.indices
      .exists(pieceId => remoteHave(pieceId) && !state.localHave(pieceId))
  }
}
