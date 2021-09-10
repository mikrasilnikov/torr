package torr.channels.test

import zio._
import torr.channels.ByteChannel
import zio.nio.core.ByteBuffer
import scala.util.Random

private case class DelayedReadRequest(buf: ByteBuffer, amount: Int, promise: Promise[Throwable, Unit])

case class TestSocketChannel(semaphore: Semaphore, rnd: Random) extends ByteChannel {

  private var incoming: Chunk[Byte] = Chunk.empty
  private var outgoing: Chunk[Byte] = Chunk.empty

  private var readRequest: Option[DelayedReadRequest] = None

  def read(buf: ByteBuffer): Task[Int] =
    semaphore.withPermit {
      for {
        bufRemaining <- buf.remaining
        bytesToRead   = rnd.between(1, bufRemaining + 1)

        promise <- Promise.make[Throwable, Unit]
        _       <- readOrDelayRequest(buf, bytesToRead, promise)
        _       <- promise.await
      } yield bytesToRead
    }

  def write(buf: ByteBuffer): Task[Int] =
    semaphore.withPermit {
      for {
        bufRemaining <- buf.remaining
        bytesToWrite  = rnd.between(1, bufRemaining + 1)

        data <- buf.getChunk(bytesToWrite)
        _     = outgoing = outgoing ++ data
      } yield bytesToWrite
    }

  def isOpen: Task[Boolean] = ZIO.succeed(true)

  private def readOrDelayRequest(
      buf: ByteBuffer,
      remaining: Int,
      promise: Promise[Throwable, Unit]
  ): Task[Unit] = {

    (remaining, incoming) match {

      case (0, _) => promise.succeed().as()

      case (_, Chunk.empty) =>
        readRequest = Some(DelayedReadRequest(buf, remaining, promise))
        ZIO.unit

      case _                =>
        val bytesToCopy = math.min(remaining, incoming.length)
        for {
          _ <- buf.putChunk(incoming.take(bytesToCopy))
          _  = incoming = incoming.drop(bytesToCopy)
          _ <- readOrDelayRequest(buf, remaining - bytesToCopy, promise)
        } yield ()
    }
  }
}

object TestSocketChannel {
  def make: Task[TestSocketChannel] =
    for {
      s  <- Semaphore.make(1)
      rnd = Random.javaRandomToRandom(new java.util.Random(42))
    } yield TestSocketChannel(s, rnd)
}
