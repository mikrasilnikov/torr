package torr.directbuffers

import zio._
import zio.actors.ActorRef
import zio.nio.core._
import FixedPoolActor._
import torr.actorsystem.ActorSystem
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.Logging

case class FixedBufferPool(private val actor: ActorRef[Command], private val hasClock: Clock)
    extends DirectBufferPool.Service {

  def allocate: Task[ByteBuffer] = {
    for {
      promise <- actor ? Allocate
      buf     <- promise.await.timeoutFail(new IllegalStateException("No direct buffers available for 10 seconds"))(
                   10.seconds
                 ).provide(hasClock)
    } yield buf
  }

  def allocateManaged: ZManaged[Any, Throwable, ByteBuffer] =
    ZManaged.make(allocate)(b => free(b).orDie)

  def free(buf: ByteBuffer): Task[Unit] = actor ! Free(buf)
  def numAvailable: Task[Int]           = actor ? GetNumAvailable
}

object FixedBufferPool {

  def make(
      maxBuffers: Int,
      bufSize: Int = torr.fileio.DefaultBufferSize
  ): ZLayer[ActorSystem with Logging with Clock, Throwable, DirectBufferPool] = {

    val effect = for {
      system   <- ZIO.service[ActorSystem.Service]
      buffers  <- ZIO.foreach(1 to maxBuffers)(i => createIndexedBuf(i, bufSize))
      actor    <- system.system.make(
                    "FixedBufferPool",
                    actors.Supervisor.none,
                    FixedPoolActor.Available(buffers.toList),
                    FixedPoolActor.stateful
                  )
      hasClock <- ZIO.environment[Clock]
    } yield FixedBufferPool(actor, hasClock)

    effect.toLayer
  }

  private def createIndexedBuf(index: Int, bufferSize: Int): UIO[ByteBuffer] =
    for {
      res <- Buffer.byteDirect(bufferSize)
      _   <- res.putInt(index)
      _   <- res.flip
    } yield res

}
