package torr.directbuffers

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.nio.core.{Buffer, ByteBuffer}

import scala.collection.mutable

object GrowablePoolActor {
  sealed trait Command[+_]         extends Any
  case object Allocate             extends Command[ByteBuffer]
  case class Free(buf: ByteBuffer) extends AnyVal with Command[Unit]
  case object GetNumAvailable      extends Command[Int]

  final case class State(buffers: mutable.Queue[ByteBuffer], poolCapacity: Int, bufferSize: Int)

  val stateful = new Stateful[Any, State, Command] {
    def receive[A](state: State, msg: Command[A], context: Context): RIO[Any, (State, A)] = {
      msg match {
        case Allocate        => allocate(state).map((state, _))
        case Free(buf)       => ZIO.succeed(free(state, buf)).as((state, ()))
        case GetNumAvailable => ZIO.succeed(state, state.buffers.length)
      }
    }
  }

  def allocate(state: State): UIO[ByteBuffer] = {
    if (state.buffers.nonEmpty) {
      val res = state.buffers.dequeue()
      res.clear.as(res)
    } else {
      Buffer.byteDirect(state.bufferSize)
    }
  }

  def free(state: State, buf: ByteBuffer): Unit = {
    if (state.buffers.size < state.poolCapacity)
      state.buffers.enqueue(buf)
    else ()
  }
}
