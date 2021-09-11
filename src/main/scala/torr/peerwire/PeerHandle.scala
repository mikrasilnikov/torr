package torr.peerwire

import zio._
import zio.nio.core._
import zio.actors.{ActorRef, Context}
import zio.clock.Clock
import zio.nio.core.channels.AsynchronousSocketChannel
import torr.actorsystem.ActorSystem
import torr.channels.{AsyncSocketChannel, ByteChannel}
import torr.directbuffers.DirectBufferPool
import torr.peerwire.PeerActor.{Command, OnMessage, State, stateful}

import scala.collection.mutable
import scala.reflect.ClassTag

case class PeerHandle(actor: ActorRef[PeerActor.Command], receiveFiber: Fiber[Throwable, Unit]) {

  def send(msg: Message): Task[Unit] = {
    actor ! PeerActor.Send(msg)
  }

  def receive(tags: ClassTag[_]*): Task[Message] = {
    for {
      promise <- actor ? PeerActor.Receive(tags: _*)
      res     <- promise.await
    } yield res
  }
}

object PeerHandle {

  def fromAddress(address: InetSocketAddress)
      : ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, PeerHandle] = {
    for {
      nioChannel <- AsynchronousSocketChannel.open
      _          <- nioChannel.connect(address).toManaged_
      channel     = AsyncSocketChannel(nioChannel)
      res        <- fromChannel(channel, address.toString())
    } yield res
  }

  def fromChannel(
      channel: ByteChannel,
      channelName: String
  ): ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, PeerHandle] = {
    for {
      actorP       <- Promise.make[Nothing, ActorRef[PeerActor.Command]].toManaged_
      receiveFiber <- receiveProc(channel, actorP).fork.toManaged(_.interrupt)

      state      = PeerActor.State(
                     channel,
                     new mutable.HashMap[ClassTag[_], Promise[Throwable, Message]],
                     new mutable.HashMap[Promise[Throwable, Message], List[ClassTag[_]]]()
                   )

      system    <- ZIO.service[ActorSystem.Service].toManaged_
      supervisor = actors.Supervisor.retryOrElse(Schedule.stop, (e, _: Unit) => ZIO.die(e))
      actor     <- system.system.make(actorName = channelName, supervisor, state, stateful)
                     .toManaged { a =>
                       for {
                         state    <- (a ? PeerActor.GetState).orDieWith(_ =>
                                       new IllegalStateException("Could not get state  from actor")
                                     )
                         context  <- (a ? PeerActor.GetContext).orDieWith(_ =>
                                       new IllegalStateException("Could not get context from actor")
                                     )
                         messages <- a.stop.orElseSucceed(List())
                         _        <- processRemainingMessages(state, context, messages.asInstanceOf[List[Command[_]]])

                       } yield ()
                     }
      _         <- actorP.succeed(actor).toManaged_

    } yield PeerHandle(actor, receiveFiber)
  }

  private def processRemainingMessages(
      state: State,
      context: Context,
      messages: List[Command[_]]
  ): ZIO[DirectBufferPool with Clock, Nothing, Unit] = {
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
      channel: ByteChannel,
      actorP: Promise[Nothing, ActorRef[Command]]
  ): ZIO[DirectBufferPool with Clock, Throwable, Unit] = {
    for {
      actor <- actorP.await
      _     <- Message.receive(channel)
                 .interruptible
                 .flatMap(msg => actor ! OnMessage(msg))
                 .forever
    } yield ()
  }
}
