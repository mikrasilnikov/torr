package torr

import zio._

package object tracker {

  case class TrackerRequest(
      announceUrl: String,
      infoHash: Array[Byte],
      peerId: Array[Byte],
      port: Int,
      uploaded: Long,
      downloaded: Long
  )

  type Tracker = Has[Tracker.Service]

  object Tracker {

    trait Service {
      def foo: Task[Unit]
    }

  }

}
