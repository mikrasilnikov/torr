package torr.bencode.test

import zio._
import zio.test._
import zio.test.Assertion._
import torr.bencode._
import java.nio.charset.StandardCharsets

object BEncodeSuite extends DefaultRunnableSpec {
  override def spec =
    suite("BEncodeSuite")(
      //
      test("Reads integer") {
        val data   = "i42e".getBytes(StandardCharsets.US_ASCII)
        val actual = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(BLong(42)))
      },
      //
      test("Reads empty string") {

        val data   = "0:".getBytes(StandardCharsets.US_ASCII)
        val actual = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(BValue.string("")))
      },
      //
      test("Reads string") {
        val data   = "5:hello".getBytes(StandardCharsets.US_ASCII)
        val actual = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(BValue.string("hello")))
      },
      //
      test("Reads empty list") {
        import BValue._
        val expected = list()
        val data     = "le".getBytes(StandardCharsets.US_ASCII)
        val actual   = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(expected))
      },
      //
      test("Reads singleton list") {
        import BValue._
        val expected = list(string("hello"))
        val data     = "l5:helloe".getBytes(StandardCharsets.US_ASCII)
        val actual   = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(expected))
      },
      //
      test("Reads list") {
        import BValue._
        val expected = list(string("hello"), long(123456))
        val data     = "l5:helloi123456ee".getBytes(StandardCharsets.US_ASCII)
        val actual   = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(expected))
      },
      //
      test("Reads empty dictionary") {
        import BValue._
        val expected = map()
        val data     = "de".getBytes(StandardCharsets.US_ASCII)
        val actual   = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(expected))
      },
      //
      test("Reads singleton dictionary") {
        import BValue._
        val expected = map(string("key1") -> string("value1"))
        val data     = "d 4:key1 6:value1 e"
          .replace(" ", "")
          .getBytes(StandardCharsets.US_ASCII)
        val actual   = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(expected))
      },
      //
      test("Reads dictionary") {
        import BValue._
        val expected = map(string("key1") -> string("value1"), string("key2") -> long(42))
        val data     = "d 4:key1 6:value1 4:key2 i42e e"
          .replace(" ", "")
          .getBytes(StandardCharsets.US_ASCII)
        val actual   = BEncode.read(Chunk.fromArray(data))
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes empty string") {
        val text       = ""
        val expected   = "0:"
        val data       = BStr(Chunk.fromArray(text.getBytes(StandardCharsets.UTF_8)))
        val serialized = BEncode.write(data)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes string") {
        val text       = "hello"
        val expected   = "5:hello"
        val data       = BStr(Chunk.fromArray(text.getBytes(StandardCharsets.UTF_8)))
        val serialized = BEncode.write(data)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes long string") {
        val text       = "hello world"
        val expected   = "11:hello world"
        val data       = BStr(Chunk.fromArray(text.getBytes(StandardCharsets.UTF_8)))
        val serialized = BEncode.write(data)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes int (zero)") {
        val integer    = 0L
        val expected   = "i0e"
        val data       = BLong(integer)
        val serialized = BEncode.write(data)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes long") {
        val integer    = Long.MaxValue
        val expected   = s"i${Long.MaxValue}e"
        val data       = BLong(integer)
        val serialized = BEncode.write(data)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes empty list") {
        val list       = BList(Nil)
        val expected   = "le"
        val serialized = BEncode.write(list)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes singleton list") {
        import BValue._
        val lst        = list(long(42))
        val expected   = "li42ee"
        val serialized = BEncode.write(lst)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes list") {
        import BValue._
        val lst        = list(long(42), string("hello"))
        val expected   = "li42e5:helloe"
        val serialized = BEncode.write(lst)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes empty map") {
        import BValue._
        val bMap       = map()
        val expected   = "de"
        val serialized = BEncode.write(bMap)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes singleton map") {
        import BValue._
        val bMap       = map((string("key"), long(42)))
        val expected   = "d3:keyi42ee"
        val serialized = BEncode.write(bMap)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      },
      //
      test("Writes map, correct key ordering") {
        import BValue._
        val bMap       = map((string("key2"), long(42)), (string("key1"), long(4242)))
        val expected   = "d4:key1i4242e4:key2i42ee"
        val serialized = BEncode.write(bMap)
        val actual     = new String(serialized.toArray, StandardCharsets.UTF_8)
        assert(actual)(equalTo(expected))
      }
    )
}
