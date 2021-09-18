package torr

import zio._
import zio.macros.accessible
import torr.peerwire.TorrBitSet

package object dispatcher {
  type DownloadDispatcher = Has[DownloadDispatcher.Service]

  type DownloadJob

  @accessible
  object DownloadDispatcher {
    trait Service {
      def tryAcquireJob(have: TorrBitSet): Task[Option[DownloadJob]]
      def releaseJob(job: DownloadJob): Task[Unit]

      def tryAcquireJobManaged(have: TorrBitSet): ZManaged[Any, Throwable, Option[DownloadJob]] =
        ZManaged.make(tryAcquireJob(have)) {
          case Some(job) => releaseJob(job).orDie
          case None      => ZIO.unit
        }
    }
  }
}
