package torr.peerwire.test

import torr.actorsystem.ActorSystemLive
import zio._
import zio.test._
import zio.test.Assertion._
import torr.channels._
import torr.directbuffers.DirectBufferPoolLive
import torr.metainfo.test.MetaInfoSpec
import torr.peerwire.{Message, TorrBitSet}
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.Buffer
import zio.test.environment.TestClock
import java.nio.charset.StandardCharsets
import java.util.Random

object MessageSpec extends DefaultRunnableSpec {
  override def spec =
    suite("MessageSuite")(
      //
      testM("send KeepAlive") {
        val expected = MetaInfoSpec.toBytes("00000000")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.KeepAlive, channel, 16)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("send Choke") {
        val expected = MetaInfoSpec.toBytes("0000000100")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Choke, channel, 16)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("send Unchoke") {
        val expected = MetaInfoSpec.toBytes("0000000101")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Unchoke, channel, 16)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("send Interested") {
        val expected = MetaInfoSpec.toBytes("0000000102")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Interested, channel, 16)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("send NotInterested") {
        val expected = MetaInfoSpec.toBytes("0000000103")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.NotInterested, channel, 16)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("send Have") {
        val expected = MetaInfoSpec.toBytes("0000000504000000ff")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Have(255), channel, 16)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("receive Have") {
        val message = MetaInfoSpec.toBytes("0000000504000000ff")
        val effect  = for {
          channel <- InMemoryChannel.make(message)
          actual  <- Message.receive(channel)
          expected = Message.Have(255)
        } yield assert(actual)(equalTo(expected))

        effect.injectCustom(
          Slf4jLogger.make((_, message) => message),
          ActorSystemLive.make("Test"),
          DirectBufferPoolLive.make(1, 1024)
        )
      },
      //
      testM("send BitField") {
        val expected = MetaInfoSpec.toBytes("0000000205a0")
        val bitSet   = TorrBitSet.make(7)
        bitSet.set.add(0)
        bitSet.set.add(2)
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.BitField(bitSet), channel, 16)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("receive BitField") {
        val message = MetaInfoSpec.toBytes("0000000205a0")
        val effect  = for {
          channel    <- InMemoryChannel.make(message)
          actual     <- Message.receive(channel)
          expectedSet = TorrBitSet.fromBytes(Chunk(0xa0.toByte))
          expected    = Message.BitField(expectedSet)
        } yield assert(actual)(equalTo(expected))

        effect.injectCustom(
          Slf4jLogger.make((_, message) => message),
          ActorSystemLive.make("Test"),
          DirectBufferPoolLive.make(1, 1024)
        )
      },
      //
      testM("send Request") {
        val expected = MetaInfoSpec.toBytes("0000000d06000000010000000200000003")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Request(1, 2, 3), channel, 32)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("receive Request") {
        val message = MetaInfoSpec.toBytes("0000000d06000000010000000200000003")
        val effect  = for {
          channel <- InMemoryChannel.make(message)
          actual  <- Message.receive(channel)
          expected = Message.Request(1, 2, 3)
        } yield assert(actual)(equalTo(expected))

        effect.injectCustom(
          Slf4jLogger.make((_, message) => message),
          ActorSystemLive.make("Test"),
          DirectBufferPoolLive.make(1, 1024)
        )
      },
      //
      testM("send Cancel") {
        val expected = MetaInfoSpec.toBytes("0000000d08000000010000000200000003")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Cancel(1, 2, 3), channel, 32)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("receive Cancel") {
        val message = MetaInfoSpec.toBytes("0000000d08000000010000000200000003")
        val effect  = for {
          channel <- InMemoryChannel.make(message)
          actual  <- Message.receive(channel)
          expected = Message.Cancel(1, 2, 3)
        } yield assert(actual)(equalTo(expected))

        effect.injectCustom(
          Slf4jLogger.make((_, message) => message),
          ActorSystemLive.make("Test"),
          DirectBufferPoolLive.make(1, 1024)
        )
      },
      //
      testM("send Port") {
        val expected = MetaInfoSpec.toBytes("0000000309ffff")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Port(65535), channel, 32)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("receive Port") {
        val message = MetaInfoSpec.toBytes("0000000309ffff")
        val effect  = for {
          channel <- InMemoryChannel.make(message)
          actual  <- Message.receive(channel)
          expected = Message.Port(65535)
        } yield assert(actual)(equalTo(expected))

        effect.injectCustom(
          Slf4jLogger.make((_, message) => message),
          ActorSystemLive.make("Test"),
          DirectBufferPoolLive.make(1, 1024)
        )
      },
      //
      testM("send Piece") {
        val payload  = Chunk.fromArray(Array[Byte](1, 2, 3, 5, 7, 11, 13, 17, 19, 23))
        val expected = MetaInfoSpec.toBytes("00000013 07 0a0b0c0d 1a1b1c1d".replace(" ", "")) ++ payload
        for {
          channel    <- InMemoryChannel.make
          payloadBuf <- Buffer.byte(payload)
          _          <- Message.send(Message.Piece(168496141, 437984285, payloadBuf), channel, 32)
          actual     <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("receive Piece") {
        val rnd   = new Random(42)
        val index = 27.toByte
        val begin = 42.toByte
        val block = torr.fileio.test.Helpers.randomChunk(rnd, 32)

        val serialized =
          Chunk[Byte](0, 0, 0, 9 + 32) ++
            Chunk[Byte](7) ++
            Chunk[Byte](0, 0, 0, index) ++
            Chunk[Byte](0, 0, 0, begin) ++
            block

        val effect = for {
          channel     <- InMemoryChannel.make(serialized)
          actual      <- Message.receive(channel)
          piece        = actual.asInstanceOf[Message.Piece]
          blockActual <- piece.block.getChunk()
        } yield assert(piece.index)(equalTo(index.toInt)) &&
          assert(piece.begin)(equalTo(begin.toInt)) &&
          assert(blockActual)(equalTo(block))

        effect.injectCustom(
          Slf4jLogger.make((_, message) => message),
          ActorSystemLive.make("Test"),
          DirectBufferPoolLive.make(1, 1024)
        )
      },
      //
      testM("send Handshake") {
        val rnd      = new Random(42)
        val reserved = Chunk.fill(8)(0.toByte)
        val infoHash = torr.fileio.test.Helpers.randomChunk(rnd, 20)
        val peerId   = torr.fileio.test.Helpers.randomChunk(rnd, 20)

        val expected =
          Chunk(19.toByte) ++
            Chunk.fromArray("BitTorrent protocol".getBytes(StandardCharsets.US_ASCII)) ++
            reserved ++
            infoHash ++
            peerId

        for {
          channel <- InMemoryChannel.make
          buf     <- Buffer.byte(128)
          msg      = Message.Handshake(infoHash, peerId)
          _       <- Message.send(msg, channel, buf)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("receive Handshake") {
        val rnd      = new Random(42)
        val reserved = Chunk.fill(8)(0.toByte)
        val infoHash = torr.fileio.test.Helpers.randomChunk(rnd, 20)
        val peerId   = torr.fileio.test.Helpers.randomChunk(rnd, 20)

        val serialized =
          Chunk(19.toByte) ++
            Chunk.fromArray("BitTorrent protocol".getBytes(StandardCharsets.US_ASCII)) ++
            reserved ++
            infoHash ++
            peerId

        for {
          channel <- InMemoryChannel.make(serialized)
          buf     <- Buffer.byte(128)
          msg     <- Message.receiveHandshake(channel, buf)
          expected = Message.Handshake(infoHash, peerId)
        } yield assert(msg)(equalTo(expected))
      }
    )
}
