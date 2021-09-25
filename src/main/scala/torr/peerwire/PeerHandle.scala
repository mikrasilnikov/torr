package torr.peerwire

import zio._
import zio.nio.core._
import zio.actors.{ActorRef, Context}
import zio.clock.Clock
import zio.nio.core.channels.AsynchronousSocketChannel
import torr.actorsystem.ActorSystem
import torr.channels.{AsyncSocketChannel, ByteChannel}
import torr.directbuffers.DirectBufferPool
import torr.peerwire.PeerActor.{Command, OnMessage, StartFailing, State, stateful}

import scala.collection.mutable
import scala.reflect.ClassTag

case class PeerHandle(actor: ActorRef[PeerActor.Command], receiveFiber: Fiber[Throwable, Unit]) {

  def send(msg: Message): Task[Unit] = {
    for {
      _ <- actor ! PeerActor.Send(msg)
    } yield ()
  }

  def receive[M <: Message](implicit tag: ClassTag[M]): Task[Message] =
    receiveCore(tag.runtimeClass)

  def receive[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Message] =
    receiveCore(tag1.runtimeClass, tag2.runtimeClass)

  def poll[M <: Message](implicit tag: ClassTag[M]): Task[Option[Message]] =
    pollCore(tag.runtimeClass)

  def onMessage(msg: Message): Task[Unit] =
    actor ? OnMessage(msg)

  private def receiveCore(classes: Class[_]*): Task[Message] = {
    for {
      p   <- Promise.make[Throwable, Message]
      _   <- actor ? PeerActor.Receive(classes.toList, p)
      res <- p.await
    } yield res
  }

  private def pollCore(classes: Class[_]*): Task[Option[Message]] = {
    actor ? PeerActor.Poll(classes.toList)
  }
}

object PeerHandle {

  private type PendingMessage[A] = (Command[A], Promise[Throwable, A])

  def fromAddress(
      address: InetSocketAddress,
      infoHash: Chunk[Byte],
      localPeerId: Chunk[Byte]
  ): ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, PeerHandle] = {
    for {
      nioChannel <- AsynchronousSocketChannel.open
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
      actorP       <- Promise.make[Nothing, ActorRef[PeerActor.Command]].toManaged_
      receiveFiber <- receiveProc(channel, msgBuf, actorP).fork.toManaged(_.interrupt)
      actor        <- createPeerActor(channel, channelName, remotePeerId, actorConfig).toManaged(shutdownPeerActor)
      _            <- actorP.succeed(actor).toManaged_
    } yield PeerHandle(actor, receiveFiber)
  }

  private def createPeerActor(
      channel: ByteChannel,
      channelName: String,
      remotePeerId: Chunk[Byte],
      actorConfig: PeerActorConfig
  ): ZIO[DirectBufferPool with Clock with ActorSystem, Throwable, ActorRef[Command]] = {

    def makeState(sendBuf: ByteBuffer) =
      PeerActor.State(
        channel,
        sendBuf,
        remotePeerId,
        new mutable.HashMap[Class[_], Promise[Throwable, Message]],
        new mutable.HashMap[Promise[Throwable, Message], List[Class[_]]](),
        actorConfig = actorConfig
      )

    // Supervisor that sends message StartFailing(...) to actor after first error.
    def makeSupervisor(promise: Promise[Nothing, ActorRef[PeerActor.Command]]) =
      actors.Supervisor.retryOrElse(
        Schedule.stop,
        (e, _: Unit) =>
          for {
            actor <- promise.await
            _     <- (actor ! StartFailing(e)).orDie
          } yield ()
      )

    for {
      actorP    <- Promise.make[Nothing, ActorRef[PeerActor.Command]]
      system    <- ZIO.service[ActorSystem.Service]
      supervisor = makeSupervisor(actorP)
      sendBuf   <- Buffer.byteDirect(1024)
      state      = makeState(sendBuf)
      actor     <- system.system.make(actorName = channelName, supervisor, state, stateful)
      _         <- actorP.succeed(actor)
    } yield actor
  }

  private def shutdownPeerActor(actor: ActorRef[Command]): URIO[DirectBufferPool with Clock, Unit] = {
    for {
      state    <- (actor ? PeerActor.GetState)
                    .orDieWith(_ => new IllegalStateException("Could not get state from actor"))
      context  <- (actor ? PeerActor.GetContext)
                    .orDieWith(_ => new IllegalStateException("Could not get context from actor"))
      messages <- actor.stop.orElseSucceed(List[PendingMessage[_]]())
      _        <- processRemainingMessages(state, context, messages.asInstanceOf[List[PendingMessage[_]]])
    } yield ()
  }

  private def processRemainingMessages(
      state: State,
      context: Context,
      messages: List[PendingMessage[_]]
  ): ZIO[DirectBufferPool with Clock, Nothing, Unit] = {
    messages match {
      case Nil          => ZIO.unit
      case (m, _) :: ms =>
        for {
          sa <- stateful.receive(state, m, context).orDieWith(e =>
                  new Exception(s"Could not process remaining message $m ($e)")
                )
          _  <- processRemainingMessages(sa._1, context, ms)
        } yield ()
    }
  }

  private[peerwire] def receiveProc(
      channel: ByteChannel,
      rcvBuf: ByteBuffer,
      actorP: Promise[Nothing, ActorRef[Command]]
  ): ZIO[DirectBufferPool with Clock, Throwable, Unit] = {
    for {
      actor <- actorP.await
      _     <- Message.receive(channel, rcvBuf).interruptible
                 .flatMap(m => actor ! OnMessage(m))
                 .forever
                 .foldM(
                   e => actor ! StartFailing(e),
                   _ => ZIO.unit
                 )
    } yield ()
  }
}
