package torr

import zio._
import zio.macros.accessible

package object dispatcher {

  type PeerId     = Chunk[Byte]
  type PieceId    = Int
  type Dispatcher = Has[Dispatcher.Service]

  sealed trait AcquireJobResult
  case class AcquireSuccess(job: DownloadJob) extends AcquireJobResult
  case object NotInterested                   extends AcquireJobResult
  case object DownloadCompleted               extends AcquireJobResult

  @accessible
  object Dispatcher {
    trait Service {
      def isDownloadCompleted: Task[Boolean]
      def isRemoteInteresting(remoteHave: Set[PieceId]): Task[Boolean]
      def registerPeer(peerId: PeerId): Task[Unit]
      def unregisterPeer(peerId: PeerId): Task[Unit]
      def acquireJob(peerId: PeerId, remoteHave: Set[PieceId]): Task[AcquireJobResult]
      def releaseJob(peerId: PeerId, job: => DownloadJob): Task[Unit]

      def acquireJobManaged(peerId: PeerId, remoteHave: Set[PieceId]): ZManaged[Any, Throwable, AcquireJobResult] =
        ZManaged.make(acquireJob(peerId, remoteHave)) {
          case AcquireSuccess(job) => releaseJob(peerId, job).orDie
          case _                   => ZIO.unit
        }
    }
  }
}
