package torr.channels

import zio._
import zio.nio.core._

import java.io.EOFException
import java.lang

case class InMemoryChannelState(data: Chunk[Byte], position: Int)

case class InMemoryChannel(state: RefM[InMemoryChannelState]) extends SeekableByteChannel {

  def read(buf: ByteBuffer): Task[Int] = state.modify(s => readCore(s, buf, s.position))

  def read(buf: ByteBuffer, position: Long): Task[Int] = state.modify(s => readCore(s, buf, position))

  def write(buf: ByteBuffer): Task[Int] = state.modify(s => writeCore(s, buf, s.position))

  def write(buf: ByteBuffer, position: Long): Task[Int] = state.modify(s => writeCore(s, buf, position))

  def isOpen: Task[Boolean] = ZIO(true)

  def getData: Task[Chunk[Byte]] = state.get.map(_.data)

  private def readCore(
      s: InMemoryChannelState,
      buf: ByteBuffer,
      position: Long
  ): Task[(Int, InMemoryChannelState)] =
    for {
      _      <- ZIO.fail(new IllegalArgumentException).when(position < 0)
      _      <- ZIO.fail(new EOFException).when(position >= s.data.length)
      bufRem <- buf.remaining
      slice   = s.data.slice(position.toInt, position.toInt + bufRem)
      _      <- buf.putChunk(slice)
    } yield (slice.size, InMemoryChannelState(s.data, position.toInt + slice.size))

  private def writeCore(
      s: InMemoryChannelState,
      buf: ByteBuffer,
      position: Long
  ): Task[(Int, InMemoryChannelState)] =
    buf.getChunk().flatMap {

      case _ if position < 0            =>
        ZIO.fail(new lang.IllegalArgumentException("Negative position"))

      case _ if position > s.data.size  =>
        ZIO.fail(new lang.IllegalArgumentException("position > size"))

      case c if position == s.data.size => // append
        ZIO.succeed(c.size, InMemoryChannelState(s.data ++ c, position.toInt + c.size))

      case c if position < s.data.size  => // overwrite + append
        val current           = s.data.toArray
        val (overlap, append) = c.splitAt(s.data.size - position.toInt)
        Array.copy(overlap.toArray, 0, current, position.toInt, overlap.length)
        val newChunk          = Chunk.fromArray(current) ++ append
        ZIO.succeed(c.size, InMemoryChannelState(newChunk, position.toInt + c.size))
    }
}

object InMemoryChannel {

  def make: UIO[InMemoryChannel] =
    for {
      state <- RefM.make(InMemoryChannelState(Chunk.empty, 0))
    } yield InMemoryChannel(state)

  def make(data: String): UIO[InMemoryChannel] =
    for {
      data  <- ZIO.succeed(Chunk.fromArray(data.toArray.map(_.toByte)))
      state <- RefM.make(InMemoryChannelState(data, 0))
    } yield InMemoryChannel(state)

  def make(data: Array[Byte]): UIO[InMemoryChannel] =
    for {
      data  <- ZIO.succeed(Chunk.fromArray(data))
      state <- RefM.make(InMemoryChannelState(data, 0))
    } yield InMemoryChannel(state)
}
