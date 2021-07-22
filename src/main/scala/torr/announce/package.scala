package torr

import zio._
import zio.clock.Clock
import zio.logging.Logging

package object announce {

  case class TrackerRequest(
      announceUrl: String,
      infoHash: Array[Byte],
      peerId: Array[Byte],
      port: Int,
      uploaded: Long,
      downloaded: Long,
      left: Long
  )

  type Announce = Has[Announce.Service]

  object Announce {
    trait Service {
      def update(request: TrackerRequest): ZIO[Clock with Logging, Throwable, TrackerResponse]
    }
  }

}
