package torr.peerwire

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.clock.Clock
import scala.collection.mutable
import torr.channels.ByteChannel
import torr.directbuffers.DirectBufferPool
import zio.nio.core.ByteBuffer
import java.time.OffsetDateTime

object PeerActor {

  sealed trait Command[+_]
  case class Send(message: Message)                                                extends Command[Unit]
  case class Receive(messageTypes: List[Class[_]], p: Promise[Throwable, Message]) extends Command[Unit]

  private[peerwire] case class OnMessage(message: Message)    extends Command[Unit]
  private[peerwire] case class StartFailing(error: Throwable) extends Command[Unit]
  private[peerwire] case object GetState                      extends Command[State]
  private[peerwire] case object GetContext                    extends Command[Context]

  case class State(
      channel: ByteChannel,
      sendBuf: ByteBuffer,
      remotePeerId: Chunk[Byte] = Chunk.fill(20)(0.toByte),
      expectedMessages: mutable.Map[Class[_], Promise[Throwable, Message]] =
        new mutable.HashMap[Class[_], Promise[Throwable, Message]](),
      givenPromises: mutable.Map[Promise[Throwable, Message], List[Class[_]]] =
        new mutable.HashMap[Promise[Throwable, Message], List[Class[_]]],
      mailbox: PeerMailbox = new PeerMailbox,
      actorConfig: PeerActorConfig = PeerActorConfig.default,
      var isFailingWith: Option[Throwable] = None
  )

  val stateful = new Stateful[DirectBufferPool with Clock, State, Command] {

    def receive[A](state: State, msg: Command[A], context: Context): RIO[DirectBufferPool with Clock, (State, A)] = {
      msg match {
        case Send(m)             => send(state, m).map(p => (state, p))
        case Receive(classes, p) => receive(state, classes, p).map(p => (state, p))
        case OnMessage(m)        =>
          m match {
            case Message.Fail => ZIO.fail(new IllegalArgumentException(s"Boom!"))
            case _            => onMessage(state, m).as(state, ())
          }
        case GetContext          => ZIO.succeed(state, context)
        case GetState            => ZIO.succeed(state, state)
        case StartFailing(error) => startFailing(state, error).as(state, ())
      }
    }

    private[peerwire] def send(
        state: State,
        msg: Message
    ): RIO[DirectBufferPool with Clock, Unit] = {
      state.isFailingWith match {
        case Some(_) => ZIO.unit
        case None    =>
          Message.send(msg, state.channel, state.sendBuf)
            .foldM(
              e => startFailing(state, e),
              _ => ZIO.unit
            )
      }
    }

    private[peerwire] def receive(
        state: State,
        classes: List[Class[_]],
        p: Promise[Throwable, Message]
    ): Task[Unit] = {

      if (state.isFailingWith.isDefined)
        p.fail(state.isFailingWith.get).as()
      else {
        state.mailbox.dequeue(classes) match {
          case Some(m) => p.succeed(m).as()
          case None    => classes.foreach { t =>
              state.expectedMessages.get(t) match {
                case Some(_) =>
                  p.fail(new IllegalStateException(s"Message of type $t is already expected by another proc")).as()
                case None    =>
                  state.expectedMessages.put(t, p)
                  state.givenPromises.put(p, classes)
              }
            }
            ZIO.unit
        }
      }
    }

    private[peerwire] def onMessage[M <: Message](state: State, msg: M): RIO[Clock, Unit] = {
      val cls = msg.getClass
      state.expectedMessages.get(cls) match {
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
      val newMailboxSize = state.mailbox.size + 1
      if (newMailboxSize > state.actorConfig.maxMailboxSize) {
        val mailboxStats = state.mailbox.formatStats
        startFailing(
          state,
          new IllegalStateException(
            s"state.mailbox.size ($newMailboxSize) > state.maxMailboxSize (${state.actorConfig.maxMailboxSize}).\n$mailboxStats"
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
          if (currentDelaySeconds >= state.actorConfig.maxMessageProcessingLatency.toSeconds) {
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
