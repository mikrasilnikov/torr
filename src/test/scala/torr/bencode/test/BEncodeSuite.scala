package torr.bencode.test

import zio._
import zio.test._
import zio.test.Assertion._
import zio.nio.core._
import torr.bencode._
import torr.channels._
import torr.channels.test._
import java.nio.charset.StandardCharsets

object BEncodeSuite extends DefaultRunnableSpec {
  override def spec =
    suite("BEncodeSuite")(
      //
      testM("Reads integer") {
        for {
          channel <- TestReadableChannel.make("i42e")
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(BInt(42)))
      },
      //
      testM("Reads empty string") {
        for {
          channel <- TestReadableChannel.make("0:")
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(BValue.string("")))
      },
      //
      testM("Reads string") {
        for {
          channel <- TestReadableChannel.make("5:hello")
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(BValue.string("hello")))
      },
      //
      testM("Reads empty list") {
        import BValue._
        val expected = list()
        for {
          channel <- TestReadableChannel.make("le")
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Reads singleton list") {
        import BValue._
        val expected = list(string("hello"))
        for {
          channel <- TestReadableChannel.make("l5:helloe")
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Reads list") {
        import BValue._
        val expected = list(string("hello"), int(123456))
        for {
          channel <- TestReadableChannel.make("l5:helloi123456ee")
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Reads empty dictionary") {
        import BValue._
        val expected = map()
        for {
          channel <- TestReadableChannel.make("de")
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Reads singleton dictionary") {
        import BValue._
        val expected = map(string("key1") -> string("value1"))
        for {
          channel <- TestReadableChannel.make("d 4:key1 6:value1 e".replace(" ", ""))
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Reads dictionary") {
        import BValue._
        val expected = map(string("key1") -> string("value1"), string("key2") -> int(42))
        for {
          channel <- TestReadableChannel.make("d 4:key1 6:value1 4:key2 i42e e".replace(" ", ""))
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes empty string") {
        val text     = ""
        val expected = "0:"
        for {
          dst    <- InMemoryWritableChannel.make
          data    = BStr(Chunk.fromArray(text.getBytes(StandardCharsets.UTF_8)))
          _      <- BEncode.write(data, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes string") {
        val text     = "hello"
        val expected = "5:hello"
        for {
          dst    <- InMemoryWritableChannel.make
          data    = BStr(Chunk.fromArray(text.getBytes(StandardCharsets.UTF_8)))
          _      <- BEncode.write(data, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes long string") {
        val text     = "hello world"
        val expected = "11:hello world"
        for {
          dst    <- InMemoryWritableChannel.make
          data    = BStr(Chunk.fromArray(text.getBytes(StandardCharsets.UTF_8)))
          buf    <- Buffer.byte(3)
          _      <- BEncode.write(data, dst, buf)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes int (zero)") {
        val integer  = 0L
        val expected = "i0e"
        for {
          dst    <- InMemoryWritableChannel.make
          data    = BInt(integer)
          _      <- BEncode.write(data, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes long") {
        val integer  = Long.MaxValue
        val expected = s"i${Long.MaxValue}e"
        for {
          dst    <- InMemoryWritableChannel.make
          data    = BInt(integer)
          _      <- BEncode.write(data, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes empty list") {
        val list     = BList(Nil)
        val expected = "le"
        for {
          dst    <- InMemoryWritableChannel.make
          _      <- BEncode.write(list, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes singleton list") {
        import BValue._
        val lst      = list(int(42))
        val expected = "li42ee"
        for {
          dst    <- InMemoryWritableChannel.make
          _      <- BEncode.write(lst, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes list") {
        import BValue._
        val lst      = list(int(42), string("hello"))
        val expected = "li42e5:helloe"
        for {
          dst    <- InMemoryWritableChannel.make
          _      <- BEncode.write(lst, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes empty map") {
        import BValue._
        val bMap     = map()
        val expected = "de"
        for {
          dst    <- InMemoryWritableChannel.make
          _      <- BEncode.write(bMap, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes singleton map") {
        import BValue._
        val bMap     = map((string("key"), int(42)))
        val expected = "d3:keyi42ee"
        for {
          dst    <- InMemoryWritableChannel.make
          _      <- BEncode.write(bMap, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes map, correct key ordering") {
        import BValue._
        val bMap     = map((string("key2"), int(42)), (string("key1"), int(4242)))
        val expected = "d4:key1i4242e4:key2i42ee"
        for {
          dst    <- InMemoryWritableChannel.make
          _      <- BEncode.write(bMap, dst)
          actual <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      }
    )
}
