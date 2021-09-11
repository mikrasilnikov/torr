package torr.channels.test

import zio._
import torr.channels.ByteChannel
import zio.nio.core.ByteBuffer
import scala.util.Random

case class TestSocketChannel(incoming: Queue[Byte], outgoing: Queue[Byte], random: Random) extends ByteChannel {

  def read(buf: ByteBuffer): Task[Int] = {
    for {
      remaining  <- buf.remaining
      bytesToRead = random.between(1, remaining + 1)
      chunk      <- incoming.takeN(bytesToRead).map(lst => Chunk.fromArray(lst.toArray))
      _          <- buf.putChunk(chunk)
    } yield bytesToRead
  }

  def write(buf: ByteBuffer): Task[Int] = {
    for {
      remaining   <- buf.remaining
      bytesToWrite = random.between(1, remaining + 1)
      chunk       <- buf.getChunk(bytesToWrite)
      _           <- outgoing.offerAll(chunk)
    } yield bytesToWrite
  }

  def isOpen: Task[Boolean] = ZIO.succeed(true)

  val remote = new ByteChannel {
    def read(buf: ByteBuffer): Task[Int] = {
      for {
        remaining  <- buf.remaining
        bytesToRead = random.between(1, remaining + 1)
        chunk      <- outgoing.takeN(bytesToRead).map(lst => Chunk.fromArray(lst.toArray))
        _          <- buf.putChunk(chunk)
      } yield bytesToRead

    }

    def write(buf: ByteBuffer): Task[Int] = {
      for {
        remaining   <- buf.remaining
        bytesToWrite = random.between(1, remaining + 1)
        chunk       <- buf.getChunk(bytesToWrite)
        _           <- incoming.offerAll(chunk)
      } yield bytesToWrite
    }

    def isOpen: Task[Boolean] = ZIO.succeed(true)
  }

}

object TestSocketChannel {
  def make: Task[TestSocketChannel] =
    for {
      incoming <- Queue.unbounded[Byte]
      outgoing <- Queue.unbounded[Byte]
      rnd       = Random.javaRandomToRandom(new java.util.Random(42))
    } yield TestSocketChannel(incoming, outgoing, rnd)
}
