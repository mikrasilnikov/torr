package torr.channels.test

import torr.channels.ByteChannel
import zio._
import zio.nio.core.ByteBuffer

case class TestWritableChannel(data: RefM[Chunk[Byte]]) extends ByteChannel {
  override def read(buf: ByteBuffer): Task[Int] =
    ZIO.fail(new Exception("TestWritableChannel does not support reading"))

  override def write(buf: ByteBuffer): Task[Int] = {
    data.modify(old =>
      for {
        chunk <- buf.getChunk()
      } yield (chunk.size, old ++ chunk)
    )
  }

  def getData: Task[Chunk[Byte]] = data.get
}

object TestWritableChannel {
  def make: UIO[TestWritableChannel] =
    for {
      data <- RefM.make[Chunk[Byte]](Chunk.empty)
    } yield TestWritableChannel(data)
}
