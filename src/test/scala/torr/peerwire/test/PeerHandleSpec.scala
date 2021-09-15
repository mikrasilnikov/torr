package torr.peerwire.test

import torr.actorsystem.ActorSystemLive
import torr.channels.test.TestSocketChannel
import torr.directbuffers.DirectBufferPoolLive
import torr.fileio.test.Helpers
import torr.peerwire.{Message, PeerHandle}
import zio._
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.Buffer
import zio.test._
import zio.test.Assertion._

import scala.reflect.ClassTag
import scala.util.Random

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
                              buf     <- Buffer.byte(1024)
                              client1 <- h.receive[Message.Choke.type].fork
                              client2 <- h.receive[Message.Unchoke.type].fork
                              _       <- Message.send(Message.NotInterested, channel.remote, buf)
                              e1      <- client1.await
                              e2      <- client2.await
                            } yield (e1, e2)
                          }
            expectedMsg = "Unexpected message of type torr.peerwire.Message$NotInterested$"

          } yield assert(e1)(fails(hasMessage(equalTo(expectedMsg)))) &&
            assert(e2)(fails(hasMessage(equalTo(expectedMsg))))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          DirectBufferPoolLive.make(8, 100)
        )
      }
    )
}
