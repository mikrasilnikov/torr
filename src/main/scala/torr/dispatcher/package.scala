package torr

import zio._
import zio.macros.accessible

package object dispatcher {

  type PieceId = Int
  type Dispatcher = Has[Dispatcher.Service]

  @accessible
  object Dispatcher {
    trait Service {
      def tryAcquireJob(have: Set[PieceId]): Task[Option[DownloadJob]]
      def releaseJob(job: DownloadJob): Task[Unit]

      def tryAcquireJobManaged(have: Set[PieceId]): ZManaged[Any, Throwable, Option[DownloadJob]] =
        ZManaged.make(tryAcquireJob(have)) {
          case Some(job) => releaseJob(job).orDie
          case None      => ZIO.unit
        }
    }
  }
}
