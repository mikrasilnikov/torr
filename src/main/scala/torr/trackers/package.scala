package torr

import zio._

package object trackers {

  case class TrackerRequest(
      announceUrl: String,
      infoHash: Array[Byte],
      peerId: Array[Byte],
      port: Int,
      uploaded: Long,
      downloaded: Long
  )

  type Trackers = Has[Trackers.Service]

  object Trackers {

    trait Service {
      def foo: Task[Unit]
    }

  }

}
