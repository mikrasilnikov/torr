package torr.peerwire.test

import torr.actorsystem.ActorSystemLive
import torr.channels.test.TestSocketChannel
import torr.directbuffers.DirectBufferPoolLive
import torr.peerwire.{Message, PeerActorConfig, PeerHandle}
import zio._
import zio.duration.durationInt
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.Buffer
import zio.test._
import zio.test.Assertion._

object PeerHandleSpec extends DefaultRunnableSpec {
  override def spec =
    suite("PeerHandleSpec")(
      //
      testM("receive keep-alive") {
        val expected = Message.KeepAlive
        val effect   = for {
          channel <- TestSocketChannel.make
          handle   = PeerHandle.fromChannel(channel, "Test", remotePeerId = Chunk.fill(20)(0.toByte))
          res     <- handle.use { h =>
                       for {
                         buf    <- Buffer.byte(1024)
                         _      <- Message.send(expected, channel.remote, buf)
                         actual <- h.receive[Message.KeepAlive.type]
                       } yield assert(actual)(equalTo(expected))
                     }
        } yield res

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          DirectBufferPoolLive.make(8)
        )
      },
      //
      testM("Disconnecting client on malformed message") {
        val rnd    = new java.util.Random(42)
        val effect = for {
          channel <- TestSocketChannel.make
          handle   = PeerHandle.fromChannel(channel, "Test", remotePeerId = Chunk.fill(20)(0.toByte))
          res     <- handle.use { h =>
                       for {
                         buf <- Buffer.byte(4)
                         _   <- buf.putInt(101) *> buf.flip
                         _   <- channel.remote.write(buf)
                         _   <- h.receive[Message.Choke.type]
                       } yield ()
                     }
        } yield ()

        val effect1 = effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          DirectBufferPoolLive.make(8, 100)
        )

        assertM(effect1.run)(fails(hasMessage(equalTo("Message size (101) > bufSize (100)"))))
      },
      //
      testM("All receiving fibers fail on protocol error") {
        val rnd    = new java.util.Random(42)
        val effect =
          for {
            channel    <- TestSocketChannel.make
            handle      = PeerHandle.fromChannel(channel, "Test", remotePeerId = Chunk.fill(20)(0.toByte))
            (e1, e2)   <- handle.use { h =>
                            for {
                              client1 <- h.receive[Message.Choke.type].fork
                              client2 <- h.receive[Message.Unchoke.type].fork
                              buf     <- Buffer.byte(4)
                              _       <- buf.putInt(101) *> buf.flip
                              _       <- channel.remote.write(buf)
                              e1      <- client1.await
                              e2      <- client2.await
                            } yield (e1, e2)
                          }
            expectedMsg = "Message size (101) > bufSize (100)"

          } yield assert(e1)(fails(hasMessage(equalTo(expectedMsg)))) &&
            assert(e2)(fails(hasMessage(equalTo(expectedMsg))))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          DirectBufferPoolLive.make(8, 100)
        )
      },
      //
      testM("All receiving fibers fail on mailbox overflow") {
        val rnd    = new java.util.Random(42)
        val effect =
          for {
            channel    <- TestSocketChannel.make
            actorConfig = PeerActorConfig(
                            maxMailboxSize = 2,
                            maxMessageProcessingLatency = 30.seconds
                          )
            handle      = PeerHandle.fromChannel(
                            channel,
                            "Test",
                            remotePeerId = Chunk.fill(20)(0.toByte),
                            actorConfig
                          )
            (e1, e2)   <- handle.use {
                            h =>
                              for {
                                client1 <- h.receive[Message.Choke.type].fork
                                client2 <- h.receive[Message.Unchoke.type].fork
                                buf     <- Buffer.byte(128)
                                _       <- Message.send(Message.KeepAlive, channel.remote, buf)
                                _       <- Message.send(Message.KeepAlive, channel.remote, buf)
                                _       <- Message.send(Message.KeepAlive, channel.remote, buf)
                                e1      <- client1.await
                                e2      <- client2.await
                              } yield (e1, e2)
                          }
            expectedMsg = "state.mailbox.size (3) > state.maxMailboxSize (2)"

          } yield assert(e1)(fails(hasMessage(containsString(expectedMsg)))) &&
            assert(e2)(fails(hasMessage(containsString(expectedMsg))))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          DirectBufferPoolLive.make(8, 100)
        )
      },
      //
      testM("Failing actor fails everything else") {
        val rnd    = new java.util.Random(42)
        val effect =
          for {
            channel    <- TestSocketChannel.make
            actorConfig = PeerActorConfig(
                            maxMailboxSize = 2,
                            maxMessageProcessingLatency = 30.seconds
                          )
            handle      = PeerHandle.fromChannel(
                            channel,
                            "Test",
                            remotePeerId = Chunk.fill(20)(0.toByte),
                            actorConfig
                          )
            e          <- handle.use { h =>
                            for {
                              client1 <- h.receive[Message.Choke.type].fork
                              buf     <- Buffer.byte(128)
                              _       <- buf.putInt(Int.MaxValue) *> buf.flip
                              _       <- channel.remote.write(buf)
                              e       <- client1.await
                            } yield e
                          }
          } yield assert(e)(fails(hasMessage(equalTo("Boom!"))))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          DirectBufferPoolLive.make(8, 100)
        )
      }
    )
}
