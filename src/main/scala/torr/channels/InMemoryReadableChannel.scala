package torr.channels

import zio._
import zio.nio.core._

case class InMemoryReadableChannel(data: RefM[Chunk[Byte]]) extends ByteChannel {
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

object InMemoryReadableChannel {
  def make(data: String): UIO[InMemoryReadableChannel] =
    for {
      data <- RefM.make(Chunk.fromArray(data.toArray.map(_.toByte)))
    } yield InMemoryReadableChannel(data)

  def make(data: Array[Byte]): UIO[InMemoryReadableChannel] =
    for {
      data <- RefM.make(Chunk.fromArray(data))
    } yield InMemoryReadableChannel(data)
}
