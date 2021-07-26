package torr.channels

import zio._
import zio.nio.channels.AsynchronousByteChannel
import zio.nio.core.ByteBuffer

case class AsyncSocketChannel(private val channel: AsynchronousByteChannel) extends ByteChannel {
  def read(buf: ByteBuffer): Task[Int]  = channel.read(buf)
  def write(buf: ByteBuffer): Task[Int] = channel.write(buf)
  def isOpen: UIO[Boolean]              = channel.isOpen
}
