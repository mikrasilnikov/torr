package torr.peerwire

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.clock.Clock
import scala.collection.mutable
import torr.channels.ByteChannel
import torr.directbuffers.DirectBufferPool
import scala.annotation.tailrec
import scala.reflect.ClassTag

object PeerActor {

  sealed trait Command[+_]
  case class Send(message: Message)              extends Command[Unit]
  case class Receive(messageTypes: ClassTag[_]*) extends Command[Promise[Throwable, Message]]

  private[peerwire] case class OnMessage(message: Message) extends Command[Unit]
  private[peerwire] case object GetState                   extends Command[State]
  private[peerwire] case object GetContext                 extends Command[Context]

  case class State(
      channel: ByteChannel,
      expectedMessages: mutable.Map[ClassTag[_], Promise[Throwable, Message]],
      givenPromises: mutable.Map[Promise[Throwable, Message], List[ClassTag[_]]]
  )

  val stateful = new Stateful[DirectBufferPool with Clock, State, Command] {

    def receive[A](state: State, msg: Command[A], context: Context): RIO[DirectBufferPool with Clock, (State, A)] = {
      msg match {
        case Send(m)                    => send(state, m).as(state, ())
        case Receive(messageTypes @ _*) => receive(state, messageTypes.toList).map(p => (state, p))
        case OnMessage(m)               => onMessage(state, m).as(state, ())
        case GetContext                 => ZIO.succeed(state, context)
        case GetState                   => ZIO.succeed(state, state)
      }
    }

    private[peerwire] def send(state: State, msg: Message): RIO[DirectBufferPool with Clock, Unit] =
      for {
        buf <- DirectBufferPool.allocate
        _   <- Message.send(msg, state.channel, buf)
        _   <- DirectBufferPool.free(buf)
      } yield ()

    private[peerwire] def receive(
        state: State,
        types: List[ClassTag[_]]
    ): ZIO[Any, Throwable, Promise[Throwable, Message]] = {

      @tailrec
      def loop(types: List[ClassTag[_]], res: Promise[Throwable, Message]): Task[Unit] = {
        types match {
          case Nil     => ZIO.unit
          case t :: ts =>
            state.expectedMessages.get(t) match {
              case Some(_) =>
                ZIO.fail(new IllegalStateException(s"Message of type $t is already expected by another proc"))
              case None    =>
                state.expectedMessages.put(t, res)
                loop(ts, res)
            }
        }
      }

      if (types.isEmpty) ZIO.fail(new IllegalArgumentException(s"Types must not be empty"))
      else
        for {
          res <- Promise.make[Throwable, Message]
          _   <- loop(types, res)
          _    = state.givenPromises.put(res, types)
        } yield res
    }

    private[peerwire] def onMessage(state: State, msg: Message): Task[Unit] = {
      val msgClass = ClassTag(msg.getClass)

      state.expectedMessages.get(msgClass) match {
        case None          => ZIO.fail(new IllegalStateException(s"Unexpected message of type $msgClass"))
        case Some(promise) =>
          for {
            _            <- promise.succeed(msg)
            expectedTypes = state.givenPromises(promise)
            _             = expectedTypes.foreach(t => state.expectedMessages.remove(t))
            _             = state.givenPromises.remove(promise)
          } yield ()
      }
    }
  }

}
