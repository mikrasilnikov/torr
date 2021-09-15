package torr.peerwire

import zio.duration.{Duration, durationInt}

case class PeerActorConfig(
    /** Will start failing if this amount of messages is enqueued in mailbox. */
    maxMailboxSize: Int,
    /** Will start failing if a received message has not been processed for this time period. */
    maxMessageProcessingLatency: Duration
)

object PeerActorConfig {
  val default: PeerActorConfig = PeerActorConfig(100, 30.seconds)
}
