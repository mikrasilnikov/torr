package torr

import zio._
import zio.macros.accessible

package object dispatcher {

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
      def acquireJob(remoteHave: Set[PieceId]): Task[AcquireJobResult]
      def releaseJob(job: DownloadJob): Task[Unit]

      def acquireJobManaged(remoteHave: Set[PieceId]): ZManaged[Any, Throwable, AcquireJobResult] =
        ZManaged.make(acquireJob(remoteHave)) {
          case AcquireSuccess(job) => releaseJob(job).orDie
          case _                   => ZIO.unit
        }
    }
  }
}
