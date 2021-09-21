package torr.directbuffers

import zio._
import zio.actors.ActorRef
import zio.nio.core._
import Actor._
import torr.actorsystem.ActorSystem
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.Logging

case class DirectBufferPoolLive(private val actor: ActorRef[Command]) extends DirectBufferPool.Service {

  def allocate: ZIO[Clock, Throwable, ByteBuffer] = {
    for {
      promise <- actor ? Allocate
      buf     <- promise.await.timeoutFail(new IllegalStateException("No direct buffers available for 10 seconds"))(
                   10.seconds
                 )
    } yield buf
  }

  def allocateManaged: ZManaged[Clock, Throwable, ByteBuffer] =
    ZManaged.make(allocate)(b => free(b).orDie)

  def free(buf: ByteBuffer): Task[Unit] = actor ! Free(buf)
  def numAvailable: Task[Int]           = actor ? GetNumAvailable
}

object DirectBufferPoolLive {

  def make(
      maxBuffers: Int,
      bufSize: Int = torr.fileio.DefaultBufferSize
  ): ZLayer[ActorSystem with Logging with Clock, Throwable, DirectBufferPool] = {

    val effect = for {
      system  <- ZIO.service[ActorSystem.Service]
      buffers <- ZIO.foreach(1 to maxBuffers)(i => createIndexedBuf(i, bufSize))
      actor   <- system.system.make(
                   "DirectBufferPoolLive",
                   actors.Supervisor.none,
                   Actor.Available(buffers.toList),
                   Actor.stateful
                 )
    } yield DirectBufferPoolLive(actor)

    effect.toLayer
  }

  private def createIndexedBuf(index: Int, bufferSize: Int): UIO[ByteBuffer] =
    for {
      res <- Buffer.byteDirect(bufferSize)
      _   <- res.putInt(index)
      _   <- res.flip
    } yield res

}
