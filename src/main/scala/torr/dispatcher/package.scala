package torr

import torr.peerwire.{Message, TorrBitSet}
import zio._
import zio.macros.accessible

package object dispatcher {

  type PeerId     = Chunk[Byte]
  type PieceId    = Int
  type Dispatcher = Has[Dispatcher.Service]

  sealed trait AcquireJobResult
  object AcquireJobResult {
    case class Success(job: DownloadJob) extends AcquireJobResult
    case object OnQueue                  extends AcquireJobResult
    case object NoInterestingPieces      extends AcquireJobResult
  }

  sealed trait ReleaseJobStatus { def job: DownloadJob }
  object ReleaseJobStatus       {
    case class Downloaded(job: DownloadJob) extends ReleaseJobStatus
    case class Choked(job: DownloadJob)     extends ReleaseJobStatus
    case class Aborted(job: DownloadJob)    extends ReleaseJobStatus
  }

  sealed trait DownloadCompletion
  object DownloadCompletion {
    case object InProgress extends DownloadCompletion
    case object EndGame    extends DownloadCompletion
    case object Completed  extends DownloadCompletion
  }

  @accessible
  object Dispatcher {
    trait Service {
      def isDownloadCompleted: Task[DownloadCompletion]
      def isRemoteInteresting(peerId: PeerId): Task[Boolean]

      /**
        * @return Queue that updates on `local have` are posted to.
        */
      def registerPeer(peerId: PeerId): Task[Dequeue[PieceId]]
      def unregisterPeer(peerId: PeerId): Task[Unit]

      def reportHave(peerId: PeerId, piece: PieceId): Task[Unit]
      def reportHaveMany(peerId: PeerId, pieces: Set[PieceId]): Task[Unit]

      def acquireJob(peerId: PeerId): Task[AcquireJobResult]
      def releaseJob(peerId: PeerId, releaseStatus: => ReleaseJobStatus): Task[Unit]

      def reportDownloadSpeed(peerId: PeerId, bytesPerSecond: Int): Task[Unit]
      def reportUploadSpeed(peerId: PeerId, bytesPerSecond: Int): Task[Unit]

      def getLocalBitField: Task[TorrBitSet]

      def numActivePeers: Task[Int]

      def acquireJobManaged(peerId: PeerId): ZManaged[Any, Throwable, AcquireJobResult] =
        ZManaged.make(acquireJob(peerId)) {
          case AcquireJobResult.Success(job) => releaseJob(peerId, ReleaseJobStatus.Choked(job)).orDie
          case _                             => ZIO.unit
        }
    }
  }
}
