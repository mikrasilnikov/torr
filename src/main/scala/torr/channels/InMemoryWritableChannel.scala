package torr.channels

import zio._
import zio.nio.core.ByteBuffer

case class InMemoryWritableChannel(data: RefM[Chunk[Byte]]) extends ByteChannel {
  override def read(buf: ByteBuffer): Task[Int] =
    ZIO.fail(new Exception("TestWritableChannel does not support reading"))

  override def write(buf: ByteBuffer): Task[Int] = {
    data.modify(old =>
      for {
        chunk <- buf.getChunk()
      } yield (chunk.size, old ++ chunk)
    )
  }

  override def isOpen: Task[Boolean] = ZIO(true)

  def getData: Task[Chunk[Byte]] = data.get
}

object InMemoryWritableChannel {
  def make: UIO[InMemoryWritableChannel] =
    for {
      data <- RefM.make[Chunk[Byte]](Chunk.empty)
    } yield InMemoryWritableChannel(data)
}
