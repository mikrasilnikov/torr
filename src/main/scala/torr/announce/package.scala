package torr

import zio._
import zio.clock.Clock
import zio.logging.Logging
import zio.macros.accessible

package object announce {

  case class TrackerRequest(
      announceUrl: String,
      infoHash: Chunk[Byte],
      peerId: Chunk[Byte],
      port: Int,
      uploaded: Long,
      downloaded: Long,
      left: Long
  )

  type Announce = Has[Announce.Service]

  @accessible
  object Announce {
    trait Service {
      def update(request: TrackerRequest): ZIO[Clock with Logging, Throwable, TrackerResponse]
    }
  }

}
