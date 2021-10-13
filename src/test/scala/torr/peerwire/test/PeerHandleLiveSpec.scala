package torr.peerwire.test

import torr.actorsystem.ActorSystemLive
import torr.channels.test.TestSocketChannel
import torr.consoleui.NullConsoleUI
import torr.directbuffers.FixedBufferPool
import torr.dispatcher.DispatcherLive
import torr.fileio.test.FileIOMock
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSpec.toBytes
import torr.peerwire.ReceiveActor.GetState
import torr.peerwire.{Message, PeerActorConfig, PeerHandle, PeerHandleLive}
import zio._
import zio.duration.durationInt
import zio.logging.Logging
import zio.magic.ZioProvideMagicOps
import zio.nio.core.Buffer
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._
import zio.test.mock.Expectation._

object PeerHandleLiveSpec extends DefaultRunnableSpec {
  override def spec =
    suite("PeerHandleLiveSpec")(
      //
      testM("Receives a message stored in mailbox") {
        val expected = Message.Have(12345)
        val effect   = for {
          channel <- TestSocketChannel.make
          msgBuf  <- Buffer.byte(64)
          handle   = PeerHandleLive.fromChannel(channel, msgBuf, "Test", remotePeerId = Chunk.fill(20)(0.toByte))
          res     <- handle.use { h =>
                       for {
                         buf           <- Buffer.byte(1024)
                         _             <- Message.send(expected, channel.remote, buf)
                         getMailboxSize = (h.receiveActor ? GetState).map(s => s.mailbox.size)
                         _             <- getMailboxSize.repeatUntil(_ == 1)
                         mailboxSize   <- getMailboxSize
                         actual        <- h.receive[Message.Have]
                       } yield assert(actual)(equalTo(expected)) && assert(mailboxSize)(equalTo(1))
                     }
        } yield res

        effect.injectCustom(
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DispatcherLive.make(10),
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      },
      //
      testM("Receives an expected message") {
        val expected = Message.Have(12345)
        val effect   = for {
          channel <- TestSocketChannel.make
          msgBuf  <- Buffer.byte(64)
          handle   = PeerHandleLive.fromChannel(channel, msgBuf, "Test", remotePeerId = Chunk.fill(20)(0.toByte))
          res     <- handle.use { h =>
                       for {
                         actualFib <- h.receive[Message.Have].fork
                         _         <- actualFib.status.repeatUntil(s => s.isInstanceOf[Fiber.Status.Suspended])
                         buf       <- Buffer.byte(1024)
                         _         <- Message.send(expected, channel.remote, buf)
                         actual    <- actualFib.join

                       } yield assert(actual)(equalTo(expected))
                     }
        } yield res

        effect.injectCustom(
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DispatcherLive.make(10),
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      },
      //
      testM("Sends a message") {
        val expected = Message.Have(12345)
        val effect   = for {
          channel <- TestSocketChannel.make
          msgBuf  <- Buffer.byte(64)
          handle   = PeerHandleLive.fromChannel(channel, msgBuf, "Test", remotePeerId = Chunk.fill(20)(0.toByte))
          res     <- handle.use { h =>
                       for {
                         _      <- h.send(expected)
                         actual <- Message.receive(channel.remote, msgBuf)
                       } yield assert(actual)(equalTo(expected))
                     }
        } yield res

        effect.injectCustom(
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DispatcherLive.make(10),
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      },
      //
      testM("Disconnects client on malformed message") {
        val rnd    = new java.util.Random(42)
        val effect = for {
          channel <- TestSocketChannel.make
          msgBuf  <- Buffer.byte(64)
          handle   = PeerHandleLive.fromChannel(channel, msgBuf, "Test", remotePeerId = Chunk.fill(20)(0.toByte))
          res     <- handle.use { h =>
                       for {
                         buf <- Buffer.byte(5)
                         _   <- buf.putInt(1)
                         _   <- buf.put(100)
                         _   <- buf.flip
                         _   <- channel.remote.write(buf)
                         _   <- h.receive[Message.Choke.type]
                       } yield ()
                     }
        } yield ()

        val effect1 = effect.injectCustom(
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DispatcherLive.make(10),
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8, 100)
        )

        assertM(effect1.run)(fails(hasMessage(equalTo("Unknown message id 100"))))
      },
      //
      testM("All receiving fibers fail on protocol error") {
        val rnd    = new java.util.Random(42)
        val effect =
          for {
            channel    <- TestSocketChannel.make
            msgBuf     <- Buffer.byte(64)
            handle      = PeerHandleLive.fromChannel(channel, msgBuf, "Test", remotePeerId = Chunk.fill(20)(0.toByte))
            (e1, e2)   <- handle.use { h =>
                            for {
                              client1 <- h.receive[Message.Choke.type].fork
                              client2 <- h.receive[Message.Unchoke.type].fork
                              buf     <- Buffer.byte(5)
                              _       <- buf.putInt(1)
                              _       <- buf.put(100)
                              _       <- buf.flip
                              _       <- channel.remote.write(buf)
                              e1      <- client1.await
                              e2      <- client2.await
                            } yield (e1, e2)
                          }
            expectedMsg = "Unknown message id 100"

          } yield assert(e1)(fails(hasMessage(equalTo(expectedMsg)))) &&
            assert(e2)(fails(hasMessage(equalTo(expectedMsg))))

        effect.injectCustom(
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DispatcherLive.make(10),
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8, 100)
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
            msgBuf     <- Buffer.byte(64)
            handle      = PeerHandleLive.fromChannel(
                            channel,
                            msgBuf,
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
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DispatcherLive.make(10),
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8, 100)
        )
      },
      //
      testM("Failing actor fails every client") {
        val rnd    = new java.util.Random(42)
        val effect =
          for {
            channel    <- TestSocketChannel.make
            actorConfig = PeerActorConfig(
                            maxMailboxSize = 2,
                            maxMessageProcessingLatency = 30.seconds
                          )
            msgBuf     <- Buffer.byte(64)
            handle      = PeerHandleLive.fromChannel(
                            channel,
                            msgBuf,
                            "Test",
                            remotePeerId = Chunk.fill(20)(0.toByte),
                            actorConfig
                          )
            e          <- handle.use { h =>
                            for {
                              client1 <- h.receive[Message.Choke.type].fork
                              buf     <- Buffer.byte(128)
                              _       <- buf.putInt(5)
                              _       <- buf.put(Byte.MaxValue)
                              _       <- buf.flip
                              _       <- channel.remote.write(buf)
                              e       <- client1.await
                            } yield e
                          }
          } yield assert(e)(fails(hasMessage(equalTo("Boom!"))))

        effect.injectCustom(
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DispatcherLive.make(10),
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8, 100)
        )
      }
    )

  private val metaInfo = MetaInfo(
    announce = "udp://tracker.openbittorrent.com:80/announce",
    pieceSize = 2 * 16 * 1024,
    entries = FileEntry(Path("file1.dat"), 4 * 16 * 1024) :: Nil,
    pieceHashes = Vector(
      // hashes of empty array
      toBytes("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
      toBytes("da39a3ee5e6b4b0d3255bfef95601890afd80709")
    ),
    infoHash = Chunk[Byte](1, 2, 3)
  )
}
