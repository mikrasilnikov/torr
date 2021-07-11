package torr.bencode.test

import torr.bencode._
import torr.channels.test.{TestReadableChannel, TestWritableChannel}
import zio._
import zio.test._
import zio.test.Assertion._
import zio.nio.core._

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
        val expected = dict()
        for {
          channel <- TestReadableChannel.make("de")
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Reads singleton dictionary") {
        import BValue._
        val expected = dict(string("key1") -> string("value1"))
        for {
          channel <- TestReadableChannel.make("d 4:key1 6:value1 e".replace(" ", ""))
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Reads dictionary") {
        import BValue._
        val expected = dict(string("key1") -> string("value1"), string("key2") -> int(42))
        for {
          channel <- TestReadableChannel.make("d 4:key1 6:value1 4:key2 i42e e".replace(" ", ""))
          actual  <- BEncode.read(channel, 3)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("Writes string") {
        val expected = "5:hello"
        for {
          src     <- TestReadableChannel.make(expected)
          dst     <- TestWritableChannel.make
          decoded <- BEncode.read(src)
          _       <- BEncode.write(decoded, dst)
          actual  <- dst.getData.map(ch => new String(ch.toArray, StandardCharsets.UTF_8))
        } yield assert(actual)(equalTo(expected))
      }
    )
}
