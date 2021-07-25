package torr.directbuffers

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.logging._
import zio.nio.core.ByteBuffer

import scala.collection.mutable

object DirectBufferPoolLiveActor {

  sealed trait Command[+_]         extends Any
  case object Allocate             extends Command[Promise[Nothing, ByteBuffer]]
  case class Free(buf: ByteBuffer) extends AnyVal with Command[Unit]
  case object GetNumAvailable      extends Command[Int]

  sealed trait State                                                         extends Any
  case class Available(buffers: List[ByteBuffer])                            extends AnyVal with State
  case class Starving(requests: mutable.Queue[Promise[Nothing, ByteBuffer]]) extends AnyVal with State

  val stateful = new Stateful[Logging, State, Command] {
    def receive[A](state: State, msg: Command[A], context: Context): RIO[Logging, (State, A)] = {
      msg match {
        case GetNumAvailable => state match {
            case Available(buffers) => ZIO.succeed(state, buffers.length)
            case Starving(requests) => ZIO.succeed(state, -1 * requests.length)
          }
        case Free(buf)       => free(buf, state).map((_, ()))
        case Allocate        => allocate(state)
      }
    }

    def free(b: ByteBuffer, state: State): Task[State] = {
      state match {

        case Available(bs)                =>
          ZIO.succeed(Available(b :: bs))

        case Starving(q) if q.length == 1 =>
          for {
            req <- ZIO(q.dequeue())
            _   <- req.succeed(b)
          } yield Available(Nil)

        case Starving(q)                  =>
          for {
            req <- ZIO(q.dequeue())
            _   <- req.succeed(b)
          } yield Starving(q)

        case _                            =>
          ZIO.fail(new IllegalStateException(s"$state"))
      }
    }

    def allocate(state: State): ZIO[Logging, Throwable, (State, Promise[Nothing, ByteBuffer])] = {

      state match {
        case Available(b :: bs) => for {
            p <- Promise.make[Nothing, ByteBuffer]
            _ <- p.succeed(b)
          } yield (Available(bs), p)

        case Available(Nil)     => for {
            _ <- log.warn("Starving for direct buffers")
            p <- Promise.make[Nothing, ByteBuffer]
          } yield (Starving(mutable.Queue(p)), p)

        case Starving(q)        => for {
            p <- Promise.make[Nothing, ByteBuffer]
            _ <- ZIO(q.enqueue(p))
          } yield (Starving(q), p)

      }
    }
  }
}
