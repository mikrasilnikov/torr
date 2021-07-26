package torr.peerwire.test

import zio._
import zio.test._
import zio.test.Assertion._
import torr.channels._
import torr.metainfo.test.MetaInfoSpec
import torr.peerwire.{Message, TorrBitSet}
import zio.nio.core.Buffer

object MessageSuite extends DefaultRunnableSpec {
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
      testM("send Request") {
        val expected = MetaInfoSpec.toBytes("0000000d06000000010000000200000003")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Request(1, 2, 3), channel, 32)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
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
      testM("send Port") {
        val expected = MetaInfoSpec.toBytes("0000000309ffff")
        for {
          channel <- InMemoryChannel.make
          _       <- Message.send(Message.Port(65535), channel, 32)
          actual  <- channel.getData
        } yield assert(actual)(equalTo(expected))
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
      }
    )
}
