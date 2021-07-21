package torr

import zio._
import zio.clock.Clock

package object tracker {

  case class TrackerRequest(
      announceUrl: String,
      infoHash: Array[Byte],
      peerId: Array[Byte],
      port: Int,
      uploaded: Long,
      downloaded: Long,
      left: Long
  )

  type Tracker = Has[Tracker.Service]

  object Tracker {
    trait Service {
      def update(request: TrackerRequest): ZIO[Clock, Throwable, TrackerResponse]
    }
  }

}
