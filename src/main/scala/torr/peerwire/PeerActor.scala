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
  case class Send(message: Message, p: Promise[Throwable, Unit])                      extends Command[Unit]
  case class Receive(messageTypes: List[ClassTag[_]], p: Promise[Throwable, Message]) extends Command[Unit]

  private[peerwire] case class OnMessage(message: Message)    extends Command[Unit]
  private[peerwire] case object GetState                      extends Command[State]
  private[peerwire] case object GetContext                    extends Command[Context]
  private[peerwire] case class StartFailing(error: Throwable) extends Command[Unit]

  case class State(
      channel: ByteChannel,
      expectedMessages: mutable.Map[ClassTag[_], Promise[Throwable, Message]],
      givenPromises: mutable.Map[Promise[Throwable, Message], List[ClassTag[_]]],
      var isFailing: Boolean = false // Connection has failed or protocol has been violated by remote side.
  )

  val stateful = new Stateful[DirectBufferPool with Clock, State, Command] {

    def receive[A](state: State, msg: Command[A], context: Context): RIO[DirectBufferPool with Clock, (State, A)] = {
      msg match {
        case Send(m, p)          => send(state, m, p).map(p => (state, p))
        case Receive(types, p)   => receive(state, types, p).map(p => (state, p))
        case OnMessage(m)        => onMessage(state, m).as(state, ())
        case GetContext          => ZIO.succeed(state, context)
        case GetState            => ZIO.succeed(state, state)
        case StartFailing(error) => startFailing(state, error).as(state, ())
      }
    }

    private[peerwire] def send(
        state: State,
        msg: Message,
        p: Promise[Throwable, Unit]
    ): RIO[DirectBufferPool with Clock, Unit] = {
      if (state.isFailing)
        p.fail(new IllegalStateException("Connection has failed or protocol has been violated")).as()
      else {
        for {
          _ <- DirectBufferPool.allocate
                 .bracket(b => DirectBufferPool.free(b).orDie) { buf =>
                   Message.send(msg, state.channel, buf)
                     .foldM(
                       e => p.fail(e) *> startFailing(state, e),
                       _ => p.succeed()
                     )
                 }
        } yield ()
      }

    }

    private[peerwire] def receive(
        state: State,
        types: List[ClassTag[_]],
        p: Promise[Throwable, Message]
    ): Task[Unit] = {

      @tailrec
      def loop(types: List[ClassTag[_]]): Task[Unit] = {
        types match {
          case Nil     => ZIO.unit
          case t :: ts =>
            state.expectedMessages.get(t) match {
              case Some(_) =>
                p.fail(new IllegalStateException(s"Message of type $t is already expected by another proc")).as()
              case None    =>
                state.expectedMessages.put(t, p)
                loop(ts)
            }
        }
      }

      if (types.isEmpty)
        p.fail(new IllegalArgumentException(s"Types must not be empty")).as()
      else if (state.isFailing)
        p.fail(new IllegalStateException("Connection has failed or protocol has been violated")).as()
      else
        for {
          _ <- loop(types)
          _  = state.givenPromises.put(p, types)
        } yield ()
    }

    private[peerwire] def onMessage(state: State, msg: Message): Task[Unit] = {
      val msgClass = ClassTag(msg.getClass)

      state.expectedMessages.get(msgClass) match {
        case None          =>
          startFailing(
            state,
            new IllegalStateException(s"Unexpected message of type $msgClass")
          )
        case Some(promise) =>
          for {
            _            <- promise.succeed(msg)
            expectedTypes = state.givenPromises(promise)
            _             = expectedTypes.foreach(t => state.expectedMessages.remove(t))
            _             = state.givenPromises.remove(promise)
          } yield ()
      }
    }

    def startFailing(state: State, error: Throwable): UIO[Unit] = {
      state.isFailing = true
      ZIO.foreach_(state.givenPromises) { case (p, _) => p.fail(error) }
    }
  }
}
