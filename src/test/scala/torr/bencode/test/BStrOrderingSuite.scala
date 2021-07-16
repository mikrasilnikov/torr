package torr.bencode.test

import torr.bencode._
import torr.channels.test.TestReadableChannel
import zio._
import zio.test.Assertion._
import zio.test._
import java.nio.charset.StandardCharsets

object BStrOrderingSuite extends DefaultRunnableSpec {
  override def spec =
    suite("BStrOrderingSuite")(
      //
      test("Less than 1") {
        val s1       = BStr(Chunk.fromArray("1".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("2".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s1, s2))(equalTo(-1))
      },
      //
      test("Less than 2") {
        val s1       = BStr(Chunk.fromArray("123".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("125".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s1, s2))(equalTo(-2))
      },
      test("Greater than 1") {
        val s1       = BStr(Chunk.fromArray("1".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("2".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s2, s1))(equalTo(1))
      },
      //
      test("Greater than 2") {
        val s1       = BStr(Chunk.fromArray("123".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("125".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s2, s1))(equalTo(2))
      },
      //
      test("Equal 1") {
        val s1       = BStr(Chunk.fromArray("".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s1, s2))(equalTo(0))
      },
      //
      test("Equal 2") {
        val s1       = BStr(Chunk.fromArray("1".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("1".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s1, s2))(equalTo(0))
      },
      //
      test("Equal 3") {
        val s1       = BStr(Chunk.fromArray("123".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("123".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s1, s2))(equalTo(0))
      },
      //
      test("Different lengths 1") {
        val s1       = BStr(Chunk.fromArray("".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("1".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s1, s2))(equalTo(-1))
      },
      //
      test("Different lengths 2") {
        val s1       = BStr(Chunk.fromArray("1".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s1, s2))(equalTo(1))
      },
      //
      test("Different lengths 3") {
        val s1       = BStr(Chunk.fromArray("123".getBytes(StandardCharsets.UTF_8)))
        val s2       = BStr(Chunk.fromArray("12345".getBytes(StandardCharsets.UTF_8)))
        val ordering = implicitly[Ordering[BStr]]
        assert(ordering.compare(s1, s2))(equalTo(-2))
      }
    )
}
