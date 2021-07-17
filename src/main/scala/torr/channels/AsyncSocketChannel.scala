package torr.channels

import zio._
import zio.nio.channels.AsynchronousByteChannel
import zio.nio.core.ByteBuffer

case class AsyncSocketChannel(private val channel: AsynchronousByteChannel) extends ByteChannel {
  override def read(buf: ByteBuffer): Task[Int]  = channel.read(buf)
  override def write(buf: ByteBuffer): Task[Int] = channel.write(buf)
  override def isOpen: UIO[Boolean]              = channel.isOpen
}
