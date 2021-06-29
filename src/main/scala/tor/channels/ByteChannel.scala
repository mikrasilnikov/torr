package tor.channels

import zio._
import zio.nio.core.ByteBuffer

trait ByteChannel {
  def read(buf: ByteBuffer): Task[Int]
}
