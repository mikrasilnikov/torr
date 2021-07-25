package torr.peerwire.test

import torr.peerwire.TorrBitSet
import zio.test.Assertion._
import zio.test._

object TorrBitSetSpec extends DefaultRunnableSpec {
  def spec =
    suite("BitFieldSuite")(
      //
      test("empty") {
        val field  = TorrBitSet.make(0)
        val res    = field.toBytes.asBits
        val actual = res.toBinaryString
        assert(actual)(equalTo(""))
      },
      //
      test("bit order") {
        val field  = TorrBitSet.make(8)
        field.set.add(0)
        field.set.add(2)
        val res    = field.toBytes.asBits
        val actual = res.toBinaryString
        assert(actual)(equalTo("10100000"))
      },
      //
      test("byte order") {
        val field  = TorrBitSet.make(16)
        field.set.add(1)
        field.set.add(8)
        field.set.add(10)
        val res    = field.toBytes.asBits
        val actual = res.toBinaryString
        assert(actual)(equalTo("01000000 10100000".replace(" ", "")))
      },
      //
      test("word order") {
        val field    = TorrBitSet.make(120)
        field.set.add(64 + 0)
        field.set.add(64 + 2)
        field.set.add(64 + 9)
        field.set.add(64 + 11)
        val res      = field.toBytes.asBits
        val actual   = res.toBinaryString
        val expected =
          "00000000 00000000 00000000 00000000 00000000 00000000 00000000 00000000" +
            "10100000 01010000 00000000 00000000 00000000 00000000 00000000"
        assert(actual)(equalTo(expected.replace(" ", "")))
      },
      //
      test("correct length") {
        val field  = TorrBitSet.make(11)
        val res    = field.toBytes.asBits
        val actual = res.toBinaryString
        assert(actual.length)(equalTo(16))
      }
    )
}
