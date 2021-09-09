package torr.peerwire

import torr.actorsystem.ActorSystem
import torr.channels.AsyncSocketChannel
import torr.directbuffers.DirectBufferPool
import zio._
import zio.actors.Actor.Stateful
import zio.actors.{ActorRef, Context}
import zio.clock.Clock
import zio.nio.core.{InetAddress, InetSocketAddress}
import zio.nio.core.channels.AsynchronousSocketChannel

import scala.collection.mutable
import scala.reflect.runtime.universe.Type

object PeerActor {

  sealed trait Command[+_]
  case object Disconnect                  extends Command[Unit]
  case class Send(message: Message)       extends Command[Unit]
  case class Receive(messageTypes: Type*) extends Command[Message]

  private[peerwire] case class OnMessage(message: Message) extends Command[Unit]
  private[peerwire] case object GetState                   extends Command[State]
  private[peerwire] case object GetContext                 extends Command[Context]

  case class State(
      channel: AsyncSocketChannel,
      expectedMessages: mutable.Map[Type, Promise[Throwable, Message]]
  )

  val stateful = new Stateful[Any, State, Command] {

    def receive[A](state: State, msg: Command[A], context: Context): RIO[Any, (State, A)] = {
      msg match {
        case Disconnect                 => ???
        case Send(message: Message)     => ???
        case Receive(messageTypes @ _*) => ???
        case OnMessage(message)         => ???
      }
    }
  }

  def connect(address: InetSocketAddress)
      : ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, ActorRef[Command]] = {
    for {
      nioChannel <- AsynchronousSocketChannel.open
      _          <- nioChannel.connect(address).toManaged_
      channel     = AsyncSocketChannel(nioChannel)

      state   = State(
                  channel,
                  new mutable.HashMap[Type, Promise[Throwable, Message]]
                )
      system <- ZIO.service[ActorSystem.Service].toManaged_
      actor  <- system.system.make(actorName = address.toString(), actors.Supervisor.none, state, stateful)
                  .toManaged { a =>
                    for {
                      state    <- (a ? GetState).orDieWith(_ =>
                                    new IllegalStateException("Could not get state  from actor")
                                  )
                      context  <- (a ? GetContext).orDieWith(_ =>
                                    new IllegalStateException("Could not get context from actor")
                                  )
                      messages <- a.stop.orElseSucceed(List())
                      _        <- processRemainingMessages(state, context, messages.asInstanceOf[List[Command[_]]])

                    } yield ()
                  }
      _      <- receiveProc(channel, actor).fork.toManaged(fib => fib.interrupt)

    } yield actor
  }

  private def processRemainingMessages(
      state: State,
      context: Context,
      messages: List[Command[_]]
  ): UIO[Unit] = {
    messages match {
      case Nil     => ZIO.unit
      case m :: ms =>
        for {
          sa <- stateful.receive(state, m, context).orDieWith(_ =>
                  new Exception("Could not process remaining messages for actor")
                )
          _  <- processRemainingMessages(sa._1, context, ms)
        } yield ()
    }
  }

  private[peerwire] def receiveProc(
      channel: AsyncSocketChannel,
      actor: ActorRef[Command]
  ): ZIO[DirectBufferPool with Clock, Throwable, Unit] = {
    for {
      _ <- Message.receive(channel)
             .flatMap(msg => actor ! OnMessage(msg))
             .forever
    } yield ()
  }

}
