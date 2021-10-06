package torr.channels

import zio._
import zio.nio.core.ByteBuffer
import zio.nio.core.channels.AsynchronousFileChannel
import zio.nio.core.file.Path

import java.io.IOException
import java.nio.file.OpenOption

case class AsyncFileChannel(
    private val channel: AsynchronousFileChannel,
    private val positionRef: RefM[Long]
) extends SeekableByteChannel {

  def read(buf: ByteBuffer): Task[Int] = {
    positionRef.modify(pos =>
      for {
        bytesRead <- channel.read(buf, pos)
      } yield (bytesRead, pos + bytesRead)
    )
  }

  def write(buf: ByteBuffer): Task[Int] = {
    positionRef.modify(pos =>
      for {
        bytesWritten <- channel.write(buf, pos)
      } yield (bytesWritten, pos + bytesWritten)
    )
  }

  def read(buf: ByteBuffer, requestedPosition: Long): Task[Int] =
    positionRef.modify(_ =>
      for {
        bytesRead <- channel.read(buf, requestedPosition)
      } yield (bytesRead, requestedPosition + bytesRead)
    )

  def write(buf: ByteBuffer, requestedPosition: Long): Task[Int] =
    positionRef.modify(_ =>
      for {
        bytesWritten <- channel.write(buf, requestedPosition)
      } yield (bytesWritten, requestedPosition + bytesWritten)
    )

  def isOpen: Task[Boolean] = channel.isOpen

  def close: Task[Unit] = channel.close
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
