package torr.channels.test

import zio._
import zio.nio.core._
import torr.channels.ByteChannel

case class TestReadableChannel(data: RefM[Chunk[Byte]]) extends ByteChannel {
  override def read(buf: ByteBuffer): Task[Int] =
    data.modify(c =>
      for {
        _  <- ZIO.fail(new Exception("no data available")).when(c.isEmpty)
        c1 <- buf.putChunk(c)
      } yield (c.size - c1.size, c1)
    )

  override def write(buf: ByteBuffer): Task[Int] = ???

  override def isOpen: Task[Boolean] = data.get.map(_.nonEmpty)
}

object TestReadableChannel {
  def make(data: String): UIO[TestReadableChannel] =
    for {
      data <- RefM.make(Chunk.fromArray(data.toArray.map(_.toByte)))
    } yield TestReadableChannel(data)

  def make(data: Array[Byte]): UIO[TestReadableChannel] =
    for {
      data <- RefM.make(Chunk.fromArray(data))
    } yield TestReadableChannel(data)
}
