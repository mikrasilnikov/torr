package torr.peerwire

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.clock.Clock
import zio.logging.Logging
import java.time.OffsetDateTime
import torr.directbuffers.DirectBufferPool

object ReceiveActor {

  sealed trait Command[+_]
  case class Receive1[M <: Message](cls: Class[_], p: Promise[Throwable, M])           extends Command[Unit]
  case class Receive2(cls1: Class[_], cls2: Class[_], p: Promise[Throwable, Message])  extends Command[Unit]
  case class ReceiveMany(messageTypes: List[Class[_]], p: Promise[Throwable, Message]) extends Command[Unit]

  case class WaitForPeerInterested(p: Promise[Throwable, Unit]) extends Command[Unit]
  case class ReceiveWhilePeerInterested[M <: Message](cls: Class[_], p: Promise[Throwable, Option[M]])
      extends Command[Unit]
  case class WaitForPeerUnchoking(p: Promise[Throwable, Unit])  extends Command[Unit]
  case class ReceiveWhilePeerUnchoking[M <: Message](cls: Class[_], p: Promise[Throwable, Option[M]])
      extends Command[Unit]

  case class Poll1[+M <: Message](cls: Class[_])                     extends Command[Option[M]]
  case class Poll1Last[M <: Message](cls: Class[_])                  extends Command[Option[M]]
  case class Poll2(cls1: Class[_], cls2: Class[_])                   extends Command[Option[Message]]
  case class Poll2Last[M <: Message](cls1: Class[_], cls2: Class[_]) extends Command[Option[Message]]
  case class PollMany(messageTypes: List[Class[_]])                  extends Command[Option[Message]]

  case class Ignore(cls: Class[_])   extends Command[Unit]
  case class Unignore(cls: Class[_]) extends Command[Unit]

  case object GetState   extends Command[ReceiveActorState]
  case object GetContext extends Command[Context]

  private[peerwire] case class OnMessage(message: Message)    extends Command[Unit]
  private[peerwire] case class StartFailing(error: Throwable) extends Command[Unit]

  val stateful = new Stateful[DirectBufferPool with Logging with Clock, ReceiveActorState, Command] {

    //noinspection WrapInsteadOfLiftInspection
    def receive[A](
        state: ReceiveActorState,
        msg: Command[A],
        context: Context
    ): RIO[DirectBufferPool with Logging with Clock, (ReceiveActorState, A)] = {
      msg match {
        case Receive1(cls, p)        => receive1(state, cls, p).map(p => (state, p))
        case Receive2(cls1, cls2, p) => receive2(state, cls1, cls2, p).map(p => (state, p))
        case ReceiveMany(classes, p) => receiveMany(state, classes, p).map(p => (state, p))

        case WaitForPeerInterested(p)           => waitForPeerInterested(state, p).as(state, ())
        case ReceiveWhilePeerInterested(cls, p) => receiveWhilePeerInterested(state, cls, p).as(state, ())

        case WaitForPeerUnchoking(p)           => waitForPeerUnchoking(state, p).as(state, ())
        case ReceiveWhilePeerUnchoking(cls, p) => receiveWhilePeerUnchoking(state, cls, p).as(state, ())

        case Ignore(cls)   => ZIO(ignore(state, cls)).as(state, ())
        case Unignore(cls) => ZIO(unignore(state, cls)).as(state, ())

        case Poll1(cls)            => ZIO(poll1(state, cls)).map(res => (state, res))
        case Poll2(cls1, cls2)     => ZIO(poll2(state, cls1, cls2)).map(res => (state, res))
        case Poll1Last(cls)        => ZIO(poll1Last(state, cls)).map(res => (state, res))
        case Poll2Last(cls1, cls2) => ZIO(poll2Last(state, cls1, cls2)).map(res => (state, res))
        case PollMany(classes)     => ZIO(pollMany(state, classes)).map(res => (state, res))

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
  }

  private[peerwire] def receive1(
      state: ReceiveActorState,
      cls: Class[_],
      p: Promise[Throwable, Message]
  ): Task[Unit] = {
    if (state.isFailingWith.isDefined)
      p.fail(state.isFailingWith.get).unit
    else if (state.expectingMessage(cls))
      p.fail(new IllegalStateException(s"Message of type $cls is already expected by another routine")).unit
    else {
      unignore1(state, cls)
      state.mailbox.dequeue1(cls) match {
        case Some(m) => p.succeed(m).unit
        case None    =>
          state.expectedMessages.put(cls, p)
          state.givenPromises.put(p, List(cls))
          ZIO.unit
      }
    }
  }

  private[peerwire] def receive2(
      state: ReceiveActorState,
      cls1: Class[_],
      cls2: Class[_],
      p: Promise[Throwable, Message]
  ): Task[Unit] = {
    if (state.isFailingWith.isDefined)
      p.fail(state.isFailingWith.get).unit
    else if (state.expectingMessage(cls1))
      p.fail(new IllegalStateException(s"Message of type $cls1 is already expected by another routine")).unit
    else if (state.expectingMessage(cls2))
      p.fail(new IllegalStateException(s"Message of type $cls2 is already expected by another routine")).unit
    else {
      unignore2(state, cls1, cls2)
      state.mailbox.dequeue2(cls1, cls2) match {
        case Some(m) => p.succeed(m).unit
        case None    =>
          state.expectedMessages.put(cls1, p)
          state.expectedMessages.put(cls2, p)
          state.givenPromises.put(p, List(cls1, cls2))
          ZIO.unit
      }
    }
  }

  private[peerwire] def receiveMany(
      state: ReceiveActorState,
      classes: List[Class[_]],
      p: Promise[Throwable, Message]
  ): Task[Unit] = {

    if (state.isFailingWith.isDefined)
      p.fail(state.isFailingWith.get).unit
    else {

      val expected = classes.filter(state.expectingMessage)
      if (expected.nonEmpty) {
        p.fail(new IllegalStateException(
          s"Messages of types ${expected.mkString(", ")} are already expected by another routine"
        )).unit

      } else {
        unignoreMany(state, classes)
        state.mailbox.dequeueMany(classes) match {
          case Some(m) => p.succeed(m).unit
          case None    => classes.foreach { t =>
              state.expectedMessages.put(t, p)
              state.givenPromises.put(p, classes)
            }
            ZIO.unit
        }
      }
    }
  }

  private[peerwire] def waitForPeerInterested(
      state: ReceiveActorState,
      p: Promise[Throwable, Unit]
  ): Task[Unit] = {
    if (state.isFailingWith.isDefined)
      p.fail(state.isFailingWith.get).unit
    else if (state.peerInterested) {
      p.succeed().unit
    } else {
      state.waitingForPeerInterested.append(p)
      ZIO.unit
    }
  }

  private[peerwire] def waitForPeerUnchoking(
      state: ReceiveActorState,
      p: Promise[Throwable, Unit]
  ): Task[Unit] = {
    if (state.isFailingWith.isDefined)
      p.fail(state.isFailingWith.get).unit
    else if (state.peerUnchoking) {
      p.succeed().unit
    } else {
      state.waitingForPeerUnchoking.append(p)
      ZIO.unit
    }
  }

  private[peerwire] def receiveWhilePeerInterested(
      state: ReceiveActorState,
      cls: Class[_],
      p: Promise[Throwable, Option[Message]]
  ): Task[Unit] = {
    if (state.isFailingWith.isDefined)
      p.fail(state.isFailingWith.get).unit
    else if (state.expectingMessage(cls))
      p.fail(new IllegalStateException(s"Message of type $cls is already expected by another routine")).unit
    else if (!state.peerInterested)
      p.succeed(None).unit
    else {
      state.mailbox.dequeue1(cls) match {
        case Some(msg) => p.succeed(Some(msg)).unit
        case None      =>
          state.expectedWhilePeerInterested.put(cls, p)
          ZIO.unit
      }
    }
  }

  private[peerwire] def receiveWhilePeerUnchoking(
      state: ReceiveActorState,
      cls: Class[_],
      p: Promise[Throwable, Option[Message]]
  ): Task[Unit] = {
    if (state.isFailingWith.isDefined)
      p.fail(state.isFailingWith.get).unit
    else if (state.expectingMessage(cls))
      p.fail(new IllegalStateException(s"Message of type $cls is already expected by another routine")).unit
    else if (!state.peerUnchoking)
      p.succeed(None).unit
    else {
      state.mailbox.dequeue1(cls) match {
        case Some(msg) => p.succeed(Some(msg)).unit
        case None      =>
          state.expectedWhilePeerUnchoking.put(cls, p)
          ZIO.unit
      }
    }
  }

  private[peerwire] def ignore(state: ReceiveActorState, cls: Class[_]): Task[Unit] = {
    if (state.isFailingWith.isDefined)
      ZIO.fail(state.isFailingWith.get)
    else {
      for {
        _ <- state.expectedMessages.get(cls) match {
               case Some(p) => p.fail(new Exception(s"Message of type $cls is ignored"))
               case None    => ZIO.unit
             }
        _ <- state.expectedWhilePeerInterested.get(cls) match {
               case Some(p) => p.fail(new Exception(s"Message of type $cls is ignored"))
               case None    => ZIO.unit
             }
        _ <- state.expectedWhilePeerUnchoking.get(cls) match {
               case Some(p) => p.fail(new Exception(s"Message of type $cls is ignored"))
               case None    => ZIO.unit
             }
        _  = poll1Last(state, cls)
        _  = state.ignoredMessages.add(cls)
      } yield ()
    }
  }

  private def unignore(state: ReceiveActorState, cls: Class[_]): Unit = {
    state.isFailingWith match {
      case Some(e) => throw e
      case None    => unignore1(state, cls)
    }
  }

  private def unignore1(state: ReceiveActorState, cls: Class[_]): Unit = {
    state.ignoredMessages.remove(cls)
  }

  private def unignore2(state: ReceiveActorState, cls1: Class[_], cls2: Class[_]): Unit = {
    state.ignoredMessages.remove(cls1)
    state.ignoredMessages.remove(cls2)
  }

  private def unignoreMany(state: ReceiveActorState, classes: List[Class[_]]): Unit = {
    classes.foreach(state.ignoredMessages.remove)
  }

  private[peerwire] def poll1(state: ReceiveActorState, cls: Class[_]): Option[Message] = {
    state.isFailingWith match {
      case Some(e) => throw e
      case None    => state.mailbox.dequeue1(cls)
    }
  }

  private[peerwire] def poll2(state: ReceiveActorState, cls1: Class[_], cls2: Class[_]): Option[Message] = {
    state.isFailingWith match {
      case Some(e) => throw e
      case None    => state.mailbox.dequeue2(cls1, cls2)
    }
  }

  private[peerwire] def poll1Last(
      state: ReceiveActorState,
      cls: Class[_]
  ): Option[Message] = {
    state.isFailingWith match {
      case Some(e) => throw e
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
        result
    }
  }

  private[peerwire] def poll2Last(
      state: ReceiveActorState,
      cls1: Class[_],
      cls2: Class[_]
  ): Option[Message] = {
    state.isFailingWith match {
      case Some(e) => throw e
      case _       =>
        var result: Option[Message] = None
        var continue                = true
        while (continue) {
          state.mailbox.dequeue2(cls1, cls2) match {
            case Some(msg) => result = Some(msg)
            case None      => continue = false
          }
        }
        result
    }
  }

  private[peerwire] def pollMany(state: ReceiveActorState, classes: List[Class[_]]): Option[Message] = {
    state.isFailingWith match {
      case Some(e) => throw e
      case None    => state.mailbox.dequeueMany(classes)
    }
  }

  //noinspection SimplifyUnlessInspection private[peerwire]
  def onMessage(state: ReceiveActorState, msg: Message): RIO[Logging with Clock, Unit] = {
    val msgClass = msg.getClass

    msg match {
      case Message.Interested    =>
        for {
          _ <- ZIO.foreach_(state.waitingForPeerInterested)(_.succeed())
          _  = state.waitingForPeerInterested.clear()
          _  = state.peerInterested = true
        } yield ()

      case Message.NotInterested =>
        for {
          _ <- ZIO.foreach_(state.expectedWhilePeerInterested) { case (_, p) => p.succeed(None) }
          _  = state.expectedWhilePeerInterested.clear()
          _  = state.peerInterested = false
        } yield ()

      case Message.Unchoke       =>
        for {
          _ <- ZIO.foreach_(state.waitingForPeerUnchoking)(_.succeed())
          _  = state.waitingForPeerUnchoking.clear()
          _  = state.peerUnchoking = true
        } yield ()

      case Message.Choke         =>
        for {
          _ <- ZIO.foreach_(state.expectedWhilePeerUnchoking) { case (_, p) => p.succeed(None) }
          _  = state.expectedWhilePeerUnchoking.clear()
          _  = state.peerUnchoking = false
        } yield ()

      case _
          if state.peerInterested &&
            state.expectedWhilePeerInterested.contains(msgClass) =>
        val promise = state.expectedWhilePeerInterested(msgClass)
        state.expectedWhilePeerInterested.remove(msgClass)
        promise.succeed(Some(msg)).unit

      case _
          if state.peerUnchoking &&
            state.expectedWhilePeerUnchoking.contains(msgClass) =>
        val promise = state.expectedWhilePeerUnchoking(msgClass)
        state.expectedWhilePeerUnchoking.remove(msgClass)
        promise.succeed(Some(msg)).unit

      case _                     =>
        onMessageGeneralCase(state, msg, msgClass)
    }
  }

  //noinspection SimplifyUnlessInspection
  private def onMessageGeneralCase(
      state: ReceiveActorState,
      msg: Message,
      msgClass: Class[_]
  ): RIO[Logging with Clock, Unit] = {
    if (state.ignoredMessages.contains(msgClass)) ZIO.unit
    else
      state.expectedMessages.get(msgClass) match {
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

  private def ensureMessagesAreBeingProcessed(
      state: ReceiveActorState,
      now: OffsetDateTime
  ): RIO[Logging with Clock, Unit] = {
    for {
      _ <- failOnExcessiveMailboxSize(state, now)
      _ <- failOnExcessiveProcessingLatency(state, now)
    } yield ()
  }

  //noinspection SimplifyWhenInspection
  private def failOnExcessiveMailboxSize(state: ReceiveActorState, now: OffsetDateTime): RIO[Logging, Unit] = {
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
  private def failOnExcessiveProcessingLatency(state: ReceiveActorState, now: OffsetDateTime): RIO[Logging, Unit] = {
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

  private def startFailing(state: ReceiveActorState, error: Throwable): RIO[Logging, Unit] = {
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
    } yield ()
  }
}
