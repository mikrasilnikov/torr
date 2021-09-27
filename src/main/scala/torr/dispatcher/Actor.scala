package torr.dispatcher

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import scala.collection.mutable
import torr.metainfo.MetaInfo

object Actor {
  sealed trait Command[+_]
  case class AcquireJob(remoteHave: Set[PieceId])          extends Command[AcquireJobResult]
  case class ReleaseJob(job: DownloadJob)                  extends Command[Unit]
  case object IsDownloadCompleted                          extends Command[Boolean]
  case class IsRemoteInteresting(remoteHave: Set[PieceId]) extends Command[Boolean]

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
      localHave: mutable.Set[PieceId],
      activeJobs: mutable.Map[PieceId, DownloadJob] = new mutable.HashMap[PieceId, DownloadJob](),
      suspendedJobs: mutable.Map[PieceId, DownloadJob] = new mutable.HashMap[PieceId, DownloadJob]()
  )

  val stateful = new Stateful[Any, State, Command] {
    //noinspection WrapInsteadOfLiftInspection
    def receive[A](state: State, msg: Command[A], context: Context): RIO[Any, (State, A)] =
      msg match {
        case AcquireJob(have)          => ZIO(acquireJob(state, have)).map(res => (state, res))
        case ReleaseJob(job)           => ZIO(releaseJob(state, job)).as(state, ())
        case IsDownloadCompleted       => ZIO(isDownloadCompleted(state)).map(res => (state, res))
        case IsRemoteInteresting(have) => ZIO(isRemoteInteresting(state, have)).map(res => (state, res))
      }
  }

  private[dispatcher] def acquireJob(state: State, remoteHave: Set[PieceId]): AcquireJobResult = {

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
          val idOption = remoteHave /*.toList.sorted*/ .find(remoteId =>
            !state.activeJobs.keySet.contains(remoteId) &&
              !state.localHave.contains(remoteId)
          )

          idOption.fold[AcquireJobResult](NotInterested) { pieceId =>
            val pieceSize =
              if (pieceId == state.metaInfo.pieceHashes.size - 1)
                state.metaInfo.torrentSize - pieceId * state.metaInfo.pieceSize
              else
                state.metaInfo.pieceSize

            val job = DownloadJob(pieceId, pieceSize.toInt)
            state.activeJobs.put(job.pieceId, job)
            AcquireSuccess(job)
          }
      }
    }
  }

  private[dispatcher] def releaseJob(state: State, job: DownloadJob): Unit = {

    if (!state.activeJobs.keySet.contains(job.pieceId)) {
      throw new IllegalArgumentException(s"$job is not in state.activeJobs")

    } else if (job.isCompleted) {
      val expected = state.metaInfo.pieceHashes(job.pieceId)
      val actual   = Chunk.fromArray(job.digest.digest())

      if (expected == actual) {
        state.activeJobs.remove(job.pieceId)
        state.localHave.add(job.pieceId)

      } else {
        // Piece hash is not equal to the expected metainfo value.
        // We must re-download piece from another peer.
        state.activeJobs.remove(job.pieceId)
        throw new IllegalStateException(s"Piece hash for piece ${job.pieceId} is not correct")
      }
    } else {
      state.activeJobs.remove(job.pieceId)
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