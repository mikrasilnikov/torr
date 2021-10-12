package torr.peerwire

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.clock.Clock

import scala.collection.mutable
import torr.channels.ByteChannel
import torr.directbuffers.DirectBufferPool
import zio.logging.Logging
import zio.nio.core.ByteBuffer

import java.time.OffsetDateTime
import scala.util.control.Breaks.break

object ReceiveActor {

  sealed trait Command[+_]
  case class Receive1[M <: Message](cls: Class[_], p: Promise[Throwable, M])           extends Command[Unit]
  case class Receive2(cls1: Class[_], cls2: Class[_], p: Promise[Throwable, Message])  extends Command[Unit]
  case class ReceiveMany(messageTypes: List[Class[_]], p: Promise[Throwable, Message]) extends Command[Unit]

  case class Poll1[+M <: Message](cls: Class[_])                     extends Command[Option[M]]
  case class Poll1Last[M <: Message](cls: Class[_])                  extends Command[Option[M]]
  case class Poll2(cls1: Class[_], cls2: Class[_])                   extends Command[Option[Message]]
  case class Poll2Last[M <: Message](cls1: Class[_], cls2: Class[_]) extends Command[Option[Message]]
  case class PollMany(messageTypes: List[Class[_]])                  extends Command[Option[Message]]

  case class Ignore(cls: Class[_]) extends Command[Unit]

  case object GetState   extends Command[State]
  case object GetContext extends Command[Context]

  private[peerwire] case class OnMessage(message: Message)    extends Command[Unit]
  private[peerwire] case class StartFailing(error: Throwable) extends Command[Unit]

  case class State(
      channel: ByteChannel,
      remotePeerId: Chunk[Byte] = Chunk.fill(20)(0.toByte),
      remotePeerIdStr: String = "default peerId",
      expectedMessages: mutable.Map[Class[_], Promise[Throwable, Message]] =
        new mutable.HashMap[Class[_], Promise[Throwable, Message]](),
      ignoredMessages: mutable.HashSet[Class[_]] = mutable.HashSet[Class[_]](),
      givenPromises: mutable.Map[Promise[Throwable, Message], List[Class[_]]] =
        new mutable.HashMap[Promise[Throwable, Message], List[Class[_]]],
      mailbox: PeerMailbox = new PeerMailbox,
      actorConfig: PeerActorConfig = PeerActorConfig.default,
      var isFailingWith: Option[Throwable] = None
  )

  val stateful = new Stateful[DirectBufferPool with Logging with Clock, State, Command] {

    def receive[A](
        state: State,
        msg: Command[A],
        context: Context
    ): RIO[DirectBufferPool with Logging with Clock, (State, A)] = {
      msg match {
        case Receive1(cls, p)        => receive1(state, cls, p).map(p => (state, p))
        case Receive2(cls1, cls2, p) => receive2(state, cls1, cls2, p).map(p => (state, p))
        case ReceiveMany(classes, p) => receiveMany(state, classes, p).map(p => (state, p))
        case Ignore(cls)             => ignore(state, cls).as(state, ())

        case Poll1(cls)            => poll1(state, cls).map(res => (state, res))
        case Poll2(cls1, cls2)     => poll2(state, cls1, cls2).map(res => (state, res))
        case Poll1Last(cls)        => poll1Last(state, cls).map(res => (state, res))
        case Poll2Last(cls1, cls2) => poll2Last(state, cls1, cls2).map(res => (state, res))

        case PollMany(classes)   => pollMany(state, classes).map(res => (state, res))
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

    private[peerwire] def receive1(
        state: State,
        cls: Class[_],
        p: Promise[Throwable, Message]
    ): Task[Unit] = {
      if (state.isFailingWith.isDefined)
        p.fail(state.isFailingWith.get).unit
      else {
        unignore1(state, cls)
        state.mailbox.dequeue1(cls) match {
          case Some(m) => p.succeed(m).unit
          case None    =>
            state.expectedMessages.get(cls) match {
              case Some(_) =>
                p.fail(new IllegalStateException(s"Message of type $cls is already expected by another proc")).unit
              case None    =>
                state.expectedMessages.put(cls, p)
                state.givenPromises.put(p, List(cls))
            }
            ZIO.unit
        }
      }
    }

    private[peerwire] def receive2(
        state: State,
        cls1: Class[_],
        cls2: Class[_],
        p: Promise[Throwable, Message]
    ): Task[Unit] = {
      if (state.isFailingWith.isDefined)
        p.fail(state.isFailingWith.get).unit
      else {
        unignore2(state, cls1, cls2)
        state.mailbox.dequeue2(cls1, cls2) match {
          case Some(m) => p.succeed(m).unit
          case None    =>
            state.expectedMessages.get(cls1)
              .orElse(state.expectedMessages.get(cls2)) match {
              case Some(c) =>
                p.fail(new IllegalStateException(s"Message of type $c is already expected by another proc")).unit
              case None    =>
                state.expectedMessages.put(cls1, p)
                state.expectedMessages.put(cls2, p)
                state.givenPromises.put(p, List(cls1, cls2))
            }
            ZIO.unit
        }
      }
    }

    private[peerwire] def receiveMany(
        state: State,
        classes: List[Class[_]],
        p: Promise[Throwable, Message]
    ): Task[Unit] = {

      if (state.isFailingWith.isDefined)
        p.fail(state.isFailingWith.get).unit
      else {
        unignoreMany(state, classes)
        state.mailbox.dequeueMany(classes) match {
          case Some(m) => p.succeed(m).unit
          case None    => classes.foreach { t =>
              state.expectedMessages.get(t) match {
                case Some(_) =>
                  p.fail(new IllegalStateException(s"Message of type $t is already expected by another proc")).unit
                case None    =>
                  state.expectedMessages.put(t, p)
                  state.givenPromises.put(p, classes)
              }
            }
            ZIO.unit
        }
      }
    }

    private def ignore(state: State, cls: Class[_]): Task[Unit] = {
      state.isFailingWith match {
        case Some(e) => ZIO.fail(e)
        case None    =>
          for {
            _ <- poll1Last(state, cls)
            _  = state.ignoredMessages.add(cls)
          } yield ()
      }
    }

    private def unignore1(state: State, cls: Class[_]): Unit = {
      state.ignoredMessages.remove(cls)
    }

    private def unignore2(state: State, cls1: Class[_], cls2: Class[_]): Unit = {
      state.ignoredMessages.remove(cls1)
      state.ignoredMessages.remove(cls2)
    }

    private def unignoreMany(state: State, classes: List[Class[_]]): Unit = {
      classes.foreach(state.ignoredMessages.remove)
    }

    private[peerwire] def poll1(state: State, cls: Class[_]): Task[Option[Message]] = {
      state.isFailingWith match {
        case Some(e) => ZIO.fail(e)
        case None    => ZIO.succeed(state.mailbox.dequeue1(cls))
      }
    }

    private[peerwire] def poll2(state: State, cls1: Class[_], cls2: Class[_]): Task[Option[Message]] = {
      state.isFailingWith match {
        case Some(e) => ZIO.fail(e)
        case None    => ZIO.succeed(state.mailbox.dequeue2(cls1, cls2))
      }
    }

    private[peerwire] def poll1Last(
        state: State,
        cls: Class[_]
    ): Task[Option[Message]] = {
      state.isFailingWith match {
        case Some(e) => ZIO.fail(e)
        case _       =>
          var result: Option[Message] = None
          var continue                = true
          while (continue) {
            state.mailbox.dequeue1(cls) match {
              case Some(msg) =>
                result = Some(msg)
              case None      =>
                continue = false
            }
          }
          ZIO.succeed(result)
      }
    }

    private[peerwire] def poll2Last(
        state: State,
        cls1: Class[_],
        cls2: Class[_]
    ): Task[Option[Message]] = {
      state.isFailingWith match {
        case Some(e) => ZIO.fail(e)
        case _       =>
          var result: Option[Message] = None
          var continue                = true
          while (continue) {
            state.mailbox.dequeue2(cls1, cls2) match {
              case Some(msg) => result = Some(msg)
              case None      => continue = false
            }
          }
          ZIO.succeed(result)
      }
    }

    private[peerwire] def pollMany(state: State, classes: List[Class[_]]): Task[Option[Message]] = {
      state.isFailingWith match {
        case Some(e) => ZIO.fail(e)
        case None    => ZIO.succeed(state.mailbox.dequeueMany(classes))
      }
    }

    //noinspection SimplifyUnlessInspection private[peerwire]
    def onMessage[M <: Message](state: State, msg: M): RIO[Logging with Clock, Unit] = {
      val cls = msg.getClass

      if (state.ignoredMessages.contains(cls)) ZIO.unit
      else
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

    private def ensureMessagesAreBeingProcessed(state: State, now: OffsetDateTime): RIO[Logging with Clock, Unit] = {
      for {
        _ <- failOnExcessiveMailboxSize(state, now)
        _ <- failOnExcessiveProcessingLatency(state, now)
      } yield ()
    }

    //noinspection SimplifyWhenInspection
    private def failOnExcessiveMailboxSize(state: State, now: OffsetDateTime): RIO[Logging, Unit] = {
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
    private def failOnExcessiveProcessingLatency(state: State, now: OffsetDateTime): RIO[Logging, Unit] = {
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

    private def startFailing(state: State, error: Throwable): RIO[Logging, Unit] = {
      state.isFailingWith = Some(error)

      val message =
        if (error.getMessage != null)
          error.getMessage.strip()
        else "null"

      for {
        _ <- Logging.debug(
               s"${state.remotePeerIdStr} ReceiveActor.startFailing(..., ${error.getClass}: $message)"
             )
        _ <- ZIO.foreach_(state.givenPromises) { case (p, _) => p.fail(error) }
        _ <- state.channel.close
      } yield ()
    }
  }
}
