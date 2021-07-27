package torr.channels.test

import torr.channels.InMemoryChannel
import zio._
import zio.nio.core._
import zio.test.Assertion._
import zio.test._

import java.io.EOFException

object InMemoryChannelSpec extends DefaultRunnableSpec {
  override def spec =
    suite("InMemoryChannelSuite")(
      //
      testM("sequential read") {
        for {
          channel <- InMemoryChannel.make(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
          buf     <- Buffer.byte(3)
          n1      <- buf.clear *> channel.read(buf)
          r1      <- buf.flip *> buf.getChunk()
          n2      <- buf.clear *> channel.read(buf)
          r2      <- buf.flip *> buf.getChunk()
          n3      <- buf.clear *> channel.read(buf)
          r3      <- buf.flip *> buf.getChunk()
          n4      <- buf.clear *> channel.read(buf)
          r4      <- buf.flip *> buf.getChunk()
        } yield assert()(anything) &&
          assert(n1, n2, n3, n4)(equalTo(3, 3, 3, 1)) &&
          assert(r1 ++ r2 ++ r3 ++ r4)(equalTo(Chunk[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9)))
      },
      //
      testM("random read") {
        for {
          channel <- InMemoryChannel.make(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
          buf     <- Buffer.byte(3)
          n1      <- buf.clear *> channel.read(buf, 1)
          r1      <- buf.flip *> buf.getChunk()
          p1      <- channel.state.get.map(_.position)
          n2      <- buf.clear *> channel.read(buf, 8)
          r2      <- buf.flip *> buf.getChunk()
          p2      <- channel.state.get.map(_.position)
        } yield assert()(anything) &&
          assert(n1, n2)(equalTo(3, 2)) &&
          assert(r1, r2)(equalTo(Chunk[Byte](1, 2, 3), Chunk[Byte](8, 9))) &&
          assert(p1, p2)(equalTo(4, 10))
      },
      //
      testM("fails when position is out of bounds 1") {
        val effect = for {
          channel <- InMemoryChannel.make(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
          buf     <- Buffer.byte(3)
          _       <- channel.read(buf, -1)
        } yield ()

        assertM(effect.run)(fails(isSubtype[IllegalArgumentException](anything)))
      },
      //
      testM("fails to read when the position is out of bounds 2") {
        val effect = for {
          channel <- InMemoryChannel.make(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
          buf     <- Buffer.byte(3)
          _       <- channel.read(buf, 10)
        } yield ()

        assertM(effect.run)(fails(isSubtype[EOFException](anything)))
      },
      //
      testM("Write - append to empty") {
        for {
          channel <- InMemoryChannel.make
          buf     <- Buffer.byte(3)
          _       <- buf.put(1) *> buf.put(2) *> buf.put(3) *> buf.flip
          _       <- channel.write(buf)
          actual  <- channel.getData
          p       <- channel.state.get.map(_.position)
        } yield assert(actual)(equalTo(Chunk[Byte](1, 2, 3))) && assert(p)(equalTo(3))
      },
      //
      testM("Write - overlap") {
        for {
          channel <- InMemoryChannel.make(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
          buf     <- Buffer.byte(3)
          _       <- buf.put(10) *> buf.put(11) *> buf.put(12) *> buf.flip
          _       <- channel.write(buf, 4)
          actual  <- channel.getData
          p       <- channel.state.get.map(_.position)
        } yield assert(actual)(equalTo(Chunk[Byte](0, 1, 2, 3, 10, 11, 12, 7, 8, 9))) &&
          assert(p)(equalTo(7))
      },
      //
      testM("Write - overlap & append") {
        for {
          channel <- InMemoryChannel.make(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
          buf     <- Buffer.byte(Chunk[Byte](10, 11, 12, 13, 14, 15))
          _       <- channel.write(buf, 7)
          actual  <- channel.getData
          p       <- channel.state.get.map(_.position)
        } yield assert(actual)(equalTo(Chunk[Byte](0, 1, 2, 3, 4, 5, 6, 10, 11, 12, 13, 14, 15))) &&
          assert(p)(equalTo(13))
      },
      //
      testM("fails to write when the position < 0") {
        val effect = for {
          channel <- InMemoryChannel.make(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
          buf     <- Buffer.byte(3)
          _       <- channel.write(buf, -1)
        } yield ()

        assertM(effect.run)(fails(isSubtype[IllegalArgumentException](anything)))
      },
      //
      testM("fails to write when the position > size") {
        val effect = for {
          channel <- InMemoryChannel.make(Array[Byte](0, 1, 2, 3, 4, 5, 6, 7, 8, 9))
          buf     <- Buffer.byte(3)
          _       <- channel.write(buf, 11)
        } yield ()

        assertM(effect.run)(fails(isSubtype[IllegalArgumentException](anything)))
      }
    )
}
