package torr.channels

import zio._
import zio.nio.core.ByteBuffer
import zio.nio.core.channels.AsynchronousFileChannel
import zio.nio.core.file.Path

import java.io.IOException
import java.nio.file.OpenOption

case class AsyncFileChannel(
    private val channel: AsynchronousFileChannel,
    private val position: RefM[Long]
) extends ByteChannel {

  override def read(buf: ByteBuffer): Task[Int] = {
    position.modify(pos =>
      for {
        bytesRead <- channel.read(buf, pos)
      } yield (bytesRead, pos + bytesRead)
    )
  }

  override def write(buf: ByteBuffer): Task[Int] = {
    position.modify(pos =>
      for {
        bytesWritten <- channel.write(buf, pos)
      } yield (bytesWritten, pos + bytesWritten)
    )
  }

}

object AsyncFileChannel {

  def open(path: Path, options: OpenOption*): Managed[IOException, AsyncFileChannel] =
    AsynchronousFileChannel.open(path, options: _*).flatMap { channel =>
      val make = for {
        position <- RefM.make(0L)
      } yield AsyncFileChannel(channel, position)

      make.toManaged(_.channel.close.ignore)
    }
}
