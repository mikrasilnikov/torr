package torr.peerwire

import zio._
import zio.nio.core._
import zio.actors.{ActorRef, Context}
import zio.clock.Clock
import zio.nio.core.channels.AsynchronousSocketChannel
import torr.actorsystem.ActorSystem
import torr.channels.{AsyncSocketChannel, ByteChannel}
import torr.directbuffers.DirectBufferPool
import torr.dispatcher.PeerId
import scala.collection.mutable
import scala.reflect.ClassTag

case class PeerHandle(
    peerId: PeerId,
    sendActor: ActorRef[SendActor.Command],
    receiveActor: ActorRef[ReceiveActor.Command],
    receiveFiber: Fiber[Throwable, Unit]
) {

  def send(msg: Message): Task[Unit] = {
    for {
      _ <- sendActor ! SendActor.Send(msg)
    } yield ()
  }

  def receive[M <: Message](implicit tag: ClassTag[M]): Task[M] =
    receiveCore(tag.runtimeClass).map(_.asInstanceOf[M])

  def receive[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Message] =
    receiveCore(tag1.runtimeClass, tag2.runtimeClass)

  def poll[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]] =
    pollCore(tag.runtimeClass).map(_.map(_.asInstanceOf[M]))

  def onMessage(msg: Message): Task[Unit] =
    receiveActor ? ReceiveActor.OnMessage(msg)

  private def receiveCore(classes: Class[_]*): Task[Message] = {
    for {
      p   <- Promise.make[Throwable, Message]
      _   <- receiveActor ? ReceiveActor.Receive(classes.toList, p)
      res <- p.await
    } yield res
  }

  private def pollCore(classes: Class[_]*): Task[Option[Message]] = {
    receiveActor ? ReceiveActor.Poll(classes.toList)
  }
}

object PeerHandle {

  private type PendingMessage[A] = (ReceiveActor.Command[A], Promise[Throwable, A])

  def fromAddress(
      address: InetSocketAddress,
      infoHash: Chunk[Byte],
      localPeerId: Chunk[Byte]
  ): ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, PeerHandle] = {
    for {
      nioChannel <- AsynchronousSocketChannel.open()
      _          <- nioChannel.connect(address).toManaged_
      channel     = AsyncSocketChannel(nioChannel)
      msgBuf     <- Buffer.byteDirect(1024).toManaged_
      res        <- fromChannelWithHandshake(channel, msgBuf, address.toString(), infoHash, localPeerId)
    } yield res
  }

  def fromChannelWithHandshake(
      channel: ByteChannel,
      msgBuf: ByteBuffer,
      channelName: String,
      infoHash: Chunk[Byte],
      localPeerId: Chunk[Byte]
  ): ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, PeerHandle] = {
    for {
      _         <- Message.sendHandshake(infoHash, localPeerId, channel, msgBuf).toManaged_
      handshake <- Message.receiveHandshake(channel, msgBuf).toManaged_
      res       <- fromChannel(channel, msgBuf, channelName, handshake.peerId)
    } yield res
  }

  def fromChannel(
      channel: ByteChannel,
      msgBuf: ByteBuffer,
      channelName: String,
      remotePeerId: Chunk[Byte],
      actorConfig: PeerActorConfig = PeerActorConfig.default
  ): ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, PeerHandle] = {
    for {
      actorP       <- Promise.make[Nothing, ActorRef[ReceiveActor.Command]].toManaged_
      receiveFiber <- receiveProc(channel, msgBuf, actorP).fork.toManaged(_.interrupt)
      receiveActor <- createReceiveActor(channel, channelName, remotePeerId, actorConfig)
                        .toManaged(shutdownReceiveActor)
      sendActor    <- createSendActor(channel, channelName, receiveActor).toManaged(_.stop.orDie)
      _            <- actorP.succeed(receiveActor).toManaged_
    } yield PeerHandle(remotePeerId, sendActor, receiveActor, receiveFiber)
  }

  private def createSendActor(
      channel: ByteChannel,
      channelName: String,
      receiveActor: ActorRef[ReceiveActor.Command]
  ): RIO[ActorSystem with Clock, ActorRef[SendActor.Command]] = {

    val supervisor =
      actors.Supervisor.retryOrElse(
        Schedule.stop,
        (e, _: Unit) =>
          for {
            _ <- (receiveActor ! ReceiveActor.StartFailing(e)).orDie
          } yield ()
      )

    for {
      system  <- ZIO.service[ActorSystem.Service]
      sendBuf <- Buffer.byteDirect(1024)
      state    = SendActor.State(
                   channel,
                   sendBuf,
                   receiveActor
                 )
      actor   <- system.system.make(actorName = s"Snd-$channelName", supervisor, state, SendActor.stateful)
    } yield actor
  }

  private def createReceiveActor(
      channel: ByteChannel,
      channelName: String,
      remotePeerId: Chunk[Byte],
      actorConfig: PeerActorConfig
  ): ZIO[DirectBufferPool with Clock with ActorSystem, Throwable, ActorRef[ReceiveActor.Command]] = {

    // Supervisor that sends message StartFailing(...) to actor after first error.
    def makeSupervisor(promise: Promise[Nothing, ActorRef[ReceiveActor.Command]]) =
      actors.Supervisor.retryOrElse(
        Schedule.stop,
        (e, _: Unit) =>
          for {
            actor <- promise.await
            _     <- (actor ! ReceiveActor.StartFailing(e)).orDie
          } yield ()
      )

    for {
      actorP    <- Promise.make[Nothing, ActorRef[ReceiveActor.Command]]
      system    <- ZIO.service[ActorSystem.Service]
      supervisor = makeSupervisor(actorP)
      state      = ReceiveActor.State(
                     channel,
                     remotePeerId,
                     actorConfig = actorConfig
                   )
      actor     <- system.system.make(actorName = s"Rcv-$channelName", supervisor, state, ReceiveActor.stateful)
      _         <- actorP.succeed(actor)
    } yield actor
  }

  private def shutdownReceiveActor(actor: ActorRef[ReceiveActor.Command]): URIO[DirectBufferPool with Clock, Unit] = {
    for {
      state    <- (actor ? ReceiveActor.GetState)
                    .orDieWith(_ => new IllegalStateException("Could not get state from actor"))
      context  <- (actor ? ReceiveActor.GetContext)
                    .orDieWith(_ => new IllegalStateException("Could not get context from actor"))
      messages <- actor.stop.orElseSucceed(List[PendingMessage[_]]())
      _        <- processRemainingMessages(state, context, messages.asInstanceOf[List[PendingMessage[_]]])
    } yield ()
  }

  private def processRemainingMessages(
      state: ReceiveActor.State,
      context: Context,
      messages: List[PendingMessage[_]]
  ): ZIO[DirectBufferPool with Clock, Nothing, Unit] = {
    messages match {
      case Nil          => ZIO.unit
      case (m, _) :: ms =>
        for {
          sa <- ReceiveActor.stateful.receive(state, m, context).orDieWith(e =>
                  new Exception(s"Could not process remaining message $m ($e)")
                )
          _  <- processRemainingMessages(sa._1, context, ms)
        } yield ()
    }
  }

  private[peerwire] def receiveProc(
      channel: ByteChannel,
      rcvBuf: ByteBuffer,
      actorP: Promise[Nothing, ActorRef[ReceiveActor.Command]]
  ): ZIO[DirectBufferPool with Clock, Throwable, Unit] = {
    for {
      actor <- actorP.await
      _     <- Message.receive(channel, rcvBuf).interruptible
                 .flatMap(m => actor ! ReceiveActor.OnMessage(m))
                 .forever
                 .foldM(
                   e => actor ! ReceiveActor.StartFailing(e),
                   _ => ZIO.unit
                 )
    } yield ()
  }
}
