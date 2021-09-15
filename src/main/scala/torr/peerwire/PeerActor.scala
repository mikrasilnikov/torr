package torr.peerwire

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.clock.Clock
import scala.collection.mutable
import torr.channels.ByteChannel
import torr.directbuffers.DirectBufferPool
import java.time.OffsetDateTime
import scala.concurrent.duration.{Duration, DurationInt}
import scala.reflect.ClassTag

object PeerActor {

  sealed trait Command[+_]
  case class Send(message: Message, p: Promise[Throwable, Unit])                      extends Command[Unit]
  case class Receive(messageTypes: List[ClassTag[_]], p: Promise[Throwable, Message]) extends Command[Unit]

  private[peerwire] case class OnMessage(message: Message)    extends Command[Unit]
  private[peerwire] case class StartFailing(error: Throwable) extends Command[Unit]
  private[peerwire] case object GetState                      extends Command[State]
  private[peerwire] case object GetContext                    extends Command[Context]

  case class State(
      channel: ByteChannel,
      remotePeerId: Chunk[Byte] = Chunk.fill(20)(0.toByte),
      expectedMessages: mutable.Map[ClassTag[_], Promise[Throwable, Message]] =
        new mutable.HashMap[ClassTag[_], Promise[Throwable, Message]](),
      givenPromises: mutable.Map[Promise[Throwable, Message], List[ClassTag[_]]] =
        new mutable.HashMap[Promise[Throwable, Message], List[ClassTag[_]]],
      mailbox: PeerMailbox = new PeerMailbox,
      /** Will start failing if this amount of messages is enqueued in mailbox. */
      maxMailboxSize: Int = 100,
      /** Will start failing if a received message has not been processed for this time period. */
      maxMessageProcessingLatency: Duration = 30.seconds,
      /** An error to fail all incoming requests with. */
      var isFailingWith: Option[Throwable] = None
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
      state.isFailingWith match {
        case Some(error) => p.fail(error).as()
        case None        =>
          for {
            _ <- DirectBufferPool.allocateManaged
                   .use { buf =>
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
        tags: List[ClassTag[_]],
        p: Promise[Throwable, Message]
    ): Task[Unit] = {

      if (state.isFailingWith.isDefined)
        p.fail(state.isFailingWith.get).as()
      else {
        state.mailbox.dequeue(tags) match {
          case Some(m) => p.succeed(m).as()
          case None    => tags.foreach { t =>
              state.expectedMessages.get(t) match {
                case Some(_) =>
                  p.fail(new IllegalStateException(s"Message of type $t is already expected by another proc")).as()
                case None    => state.expectedMessages.put(t, p)
              }
            }
            ZIO.unit
        }
      }
    }

    private[peerwire] def onMessage[M <: Message](state: State, msg: M)(implicit tag: ClassTag[M]): RIO[Clock, Unit] = {
      state.expectedMessages.get(tag) match {
        case None          =>
          for {
            now <- clock.currentDateTime
            _   <- ensureMessagesAreBeingProcessed(state, now)
            _    = state.mailbox.enqueue(now, msg)
          } yield ()

        case Some(promise) =>
          for {
            _            <- promise.succeed(msg)
            expectedTypes = state.givenPromises(promise)
            _             = expectedTypes.foreach(t => state.expectedMessages.remove(t))
            _             = state.givenPromises.remove(promise)
          } yield ()
      }
    }

    private def ensureMessagesAreBeingProcessed(state: State, now: OffsetDateTime): RIO[Clock, Unit] = {
      for {
        _ <- failOnExcessiveMailboxSize(state, now)
        _ <- failOnExcessiveProcessingLatency(state, now)
      } yield ()
    }

    //noinspection SimplifyWhenInspection
    private def failOnExcessiveMailboxSize(state: State, now: OffsetDateTime): Task[Unit] = {
      if (state.mailbox.size >= state.maxMailboxSize) {
        val mailboxStats = state.mailbox.formatStats
        startFailing(
          state,
          new IllegalStateException(
            s"state.mailbox.size (${state.mailbox.size}) >= state.maxMailboxSize (${state.maxMailboxSize}).\n$mailboxStats"
          )
        )
      } else ZIO.unit
    }

    //noinspection SimplifyWhenInspection
    private def failOnExcessiveProcessingLatency(state: State, now: OffsetDateTime): Task[Unit] = {
      state.mailbox.oldestMessage match {
        case None              => ZIO.unit
        case Some((time, msg)) =>
          val currentDelaySeconds = java.time.Duration.between(time, now).toSeconds
          if (currentDelaySeconds >= state.maxMessageProcessingLatency.toSeconds) {
            val mailboxStats = state.mailbox.formatStats
            startFailing(
              state,
              new IllegalStateException(
                s"A message $msg nas not been processed for $currentDelaySeconds seconds.\n$mailboxStats"
              )
            )
          } else ZIO.unit
      }
    }

    private def startFailing(state: State, error: Throwable): UIO[Unit] = {
      state.isFailingWith = Some(error)
      ZIO.foreach_(state.givenPromises) { case (p, _) => p.fail(error) }
    }
  }
}
