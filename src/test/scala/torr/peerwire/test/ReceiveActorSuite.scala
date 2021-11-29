package torr.peerwire.test

import torr.peerwire.{Message, ReceiveActor, ReceiveActorState}
import zio._
import zio.logging.Logging
import zio.test._
import zio.test.Assertion._

object ReceiveActorSuite extends DefaultRunnableSpec {
  def spec =
    suite("ReceiveActorSuite")(
      //
      testM("ignore fails waiting routine") {
        val state = ReceiveActorState()
        for {
          p      <- Promise.make[Throwable, Message]
          _      <- ReceiveActor.receive1(state, Message.KeepAlive.getClass, p)
          _      <- ReceiveActor.ignore(state, Message.KeepAlive.getClass)
          actual <- p.await.fork.flatMap(_.await)
        } yield assert(actual)(
          fails(hasMessage(equalTo("Message of type class torr.peerwire.Message$KeepAlive$ is ignored")))
        )
      },
      //
      testM("waitForPeerInterested - instant") {
        val state = ReceiveActorState(peerInterested = true)
        for {
          p      <- Promise.make[Throwable, Unit]
          _      <- ReceiveActor.waitForPeerInterested(state, p)
          actual <- p.await.fork.flatMap(_.await)
        } yield assert(actual)(succeeds(anything))
      },
      //
      testM("waitForPeerUnchoking - instant") {
        val state = ReceiveActorState(peerUnchoking = true)
        for {
          p      <- Promise.make[Throwable, Unit]
          _      <- ReceiveActor.waitForPeerUnchoking(state, p)
          actual <- p.await.fork.flatMap(_.await)
        } yield assert(actual)(succeeds(anything))
      },
      //
      testM("waitForPeerInterested - delayed") {
        val state = ReceiveActorState(peerInterested = false)
        for {
          p       <- Promise.make[Throwable, Unit]
          _       <- ReceiveActor.waitForPeerInterested(state, p)
          actual1 <- p.poll
          _       <- ReceiveActor.onMessage(state, Message.Interested)
                       .provideCustomLayer(Logging.ignore)
          actual2 <- p.poll
        } yield assert(actual1)(isNone) && assert(actual2)(isSome)
      },
      //
      testM("waitForPeerUnchoking - delayed") {
        val state = ReceiveActorState(peerUnchoking = false)
        for {
          p       <- Promise.make[Throwable, Unit]
          _       <- ReceiveActor.waitForPeerUnchoking(state, p)
          actual1 <- p.poll
          _       <- ReceiveActor.onMessage(state, Message.Unchoke)
                       .provideCustomLayer(Logging.ignore)
          actual2 <- p.poll
        } yield assert(actual1)(isNone) && assert(actual2)(isSome)
      },
      //
      testM("receiveWhilePeerInterested - message is expected by another routine") {
        val state = ReceiveActorState()
        for {
          p0 <- Promise.make[Throwable, Message]
          _  <- ReceiveActor.receive1(state, Message.KeepAlive.getClass, p0)

          p1 <- Promise.make[Throwable, Option[Message]]
          _  <- ReceiveActor.receiveWhilePeerInterested(state, Message.KeepAlive.getClass, p1)

          actual <- p1.await.fork.flatMap(_.await)

        } yield assert(actual)(fails(hasMessage(
          equalTo("Message of type class torr.peerwire.Message$KeepAlive$ is already expected by another routine")
        )))
      },
      //
      testM("receiveWhilePeerUnchoking - message is expected by another routine") {
        val state = ReceiveActorState()
        for {
          p0 <- Promise.make[Throwable, Message]
          _  <- ReceiveActor.receive1(state, Message.KeepAlive.getClass, p0)

          p1 <- Promise.make[Throwable, Option[Message]]
          _  <- ReceiveActor.receiveWhilePeerUnchoking(state, Message.KeepAlive.getClass, p1)

          actual <- p1.await.fork.flatMap(_.await)

        } yield assert(actual)(fails(hasMessage(
          equalTo("Message of type class torr.peerwire.Message$KeepAlive$ is already expected by another routine")
        )))
      },
      //
      testM("receiveWhilePeerInterested - instant from mailbox") {
        val state = ReceiveActorState(peerInterested = true)
        for {
          _      <- ReceiveActor.onMessage(state, Message.KeepAlive).provideCustomLayer(Logging.ignore)
          p      <- Promise.make[Throwable, Option[Message]]
          _      <- ReceiveActor.receiveWhilePeerInterested(state, Message.KeepAlive.getClass, p)
          actual <- p.await
        } yield assert(actual)(isSome(equalTo(Message.KeepAlive)))
      },
      //
      testM("receiveWhilePeerUnchoking - instant from mailbox") {
        val state = ReceiveActorState(peerUnchoking = true)
        for {
          _      <- ReceiveActor.onMessage(state, Message.KeepAlive).provideCustomLayer(Logging.ignore)
          p      <- Promise.make[Throwable, Option[Message]]
          _      <- ReceiveActor.receiveWhilePeerUnchoking(state, Message.KeepAlive.getClass, p)
          actual <- p.await
        } yield assert(actual)(isSome(equalTo(Message.KeepAlive)))
      },
      //
      testM("receiveWhilePeerInterested - instant from wire") {
        val state = ReceiveActorState(peerInterested = true)
        for {
          p      <- Promise.make[Throwable, Option[Message]]
          _      <- ReceiveActor.receiveWhilePeerInterested(state, Message.KeepAlive.getClass, p)
          _      <- ReceiveActor.onMessage(state, Message.KeepAlive).provideCustomLayer(Logging.ignore)
          actual <- p.await
        } yield assert(actual)(isSome(equalTo(Message.KeepAlive)))
      },
      //
      testM("receiveWhilePeerUnchoking - instant from wire") {
        val state = ReceiveActorState(peerUnchoking = true)
        for {
          p      <- Promise.make[Throwable, Option[Message]]
          _      <- ReceiveActor.receiveWhilePeerUnchoking(state, Message.KeepAlive.getClass, p)
          _      <- ReceiveActor.onMessage(state, Message.KeepAlive).provideCustomLayer(Logging.ignore)
          actual <- p.await
        } yield assert(actual)(isSome(equalTo(Message.KeepAlive)))
      }
    )
}
