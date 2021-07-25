package torr.directbuffers

import zio._
import zio.actors.ActorRef
import zio.nio.core._
import DirectBufferPoolLiveActor._
import torr.actorsystem.ActorSystem
import zio.clock.Clock
import zio.logging.Logging

case class DirectBufferPoolLive(private val actor: ActorRef[Command]) extends DirectBufferPool.Service {

  def allocate: Task[ByteBuffer] =
    for {
      promise <- actor ? Allocate
      buf     <- promise.await
    } yield buf

  def free(buf: ByteBuffer): Task[Unit] = actor ! Free(buf)
  def numAvailable: Task[Int]           = actor ? GetNumAvailable
}

object DirectBufferPoolLive {

  private val bufferSize = 32 * 1024;

  def make(maxBuffers: Int): ZLayer[ActorSystem with Clock with Logging, Throwable, DirectBufferPool] = {

    val effect = for {
      system  <- ZIO.service[ActorSystem.Service]
      buffers <- ZIO.foreach(1 to maxBuffers)(createIndexedBuf)
      actor   <- system.system.make(
                   "DirectBufferPoolLive",
                   actors.Supervisor.none,
                   DirectBufferPoolLiveActor.Available(buffers.toList),
                   DirectBufferPoolLiveActor.stateful
                 )
    } yield DirectBufferPoolLive(actor)

    effect.toLayer
  }

  // Putting buffer index at the beginning, for testing
  def createIndexedBuf(index: Int): UIO[ByteBuffer] =
    for {
      res <- Buffer.byte(bufferSize)
      _   <- res.putInt(index)
      _   <- res.flip
    } yield res

}
