package torr.directbuffers

import torr.actorsystem.ActorSystem
import zio._
import zio.actors.ActorRef
import zio.clock.Clock
import zio.nio.core.ByteBuffer
import scala.collection.mutable

case class GrowableBufferPool(private val actor: ActorRef[GrowablePoolActor.Command]) extends DirectBufferPool.Service {
  def allocate: Task[ByteBuffer]        = actor ? GrowablePoolActor.Allocate
  def free(buf: ByteBuffer): Task[Unit] = actor ! GrowablePoolActor.Free(buf)
  def numAvailable: Task[Int]           = actor ? GrowablePoolActor.GetNumAvailable
}

object GrowableBufferPool {
  def make(
      capacity: Int,
      bufSize: Int = torr.fileio.DefaultBufferSize
  ): ZLayer[ActorSystem with Clock, Throwable, DirectBufferPool] = {

    val effect = for {
      system <- ZIO.service[ActorSystem.Service]
      actor  <- system.system.make(
                  "GrowableBufferPool",
                  actors.Supervisor.none,
                  GrowablePoolActor.State(new mutable.Queue[ByteBuffer](), capacity, bufSize),
                  GrowablePoolActor.stateful
                )
    } yield GrowableBufferPool(actor)

    effect.toLayer
  }
}
