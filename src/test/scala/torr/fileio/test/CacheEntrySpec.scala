package torr.fileio.test

import torr.fileio.{EntryAddr, IntRange, WriteEntry}
import zio._
import zio.nio.core.{Buffer, ByteBuffer}
import zio.test._
import zio.test.Assertion._

import scala.util.Random

object CacheEntrySpec extends DefaultRunnableSpec {
  override def spec =
    suite("CacheEntrySpec")(
      //
      testM("WriteEntry.freeRanges - empty entry") {
        for {
          (b0, _) <- randomBuf(16)
          entry    = WriteEntry(EntryAddr(0, 0), b0, 16)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(0, 16))))
      },
      testM("WriteEntry.freeRanges - first half used") {
        for {
          (b0, d0) <- randomBuf(16)
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16)
          (b1, d1) <- randomBuf(8)
          _        <- entry.write(b1, 0, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(8, 16)))) &&
          assert(resData)(equalTo(d1 ++ d0.drop(8)))
      },
      testM("WriteEntry.freeRanges - last half used") {
        for {
          (b0, d0) <- randomBuf(16)
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16)
          (b1, d1) <- randomBuf(8)
          _        <- entry.write(b1, 8, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(0, 8)))) &&
          assert(resData)(equalTo(d0.take(8) ++ d1))
      },
      testM("WriteEntry.freeRanges - free, used, free") {
        for {
          (b0, d0) <- randomBuf(16)
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16)
          (b1, d1) <- randomBuf(8)
          _        <- entry.write(b1, 4, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(12, 16), IntRange(0, 4)))) &&
          assert(resData)(equalTo(d0.take(4) ++ d1 ++ d0.drop(12)))
      },
      testM("WriteEntry.freeRanges - used, free, used") {
        for {
          (b0, d0) <- randomBuf(16)
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16)
          (b1, d1) <- randomBuf(8)
          _        <- entry.write(b1, 0, 4)
          _        <- entry.write(b1, 12, 4)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(4, 12)))) &&
          assert(resData)(equalTo(d1.take(4) ++ d0.drop(4).take(8) ++ d1.take(4)))
      },
      testM("WriteEntry.freeRanges - used overlaps used, free") {
        for {
          (b0, d0) <- randomBuf(16)
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16)
          (b1, d1) <- randomBuf(8)
          _        <- entry.write(b1, 0, 8)
          _        <- entry.write(b1, 4, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(12, 16)))) &&
          assert(resData)(equalTo(d1.take(4) ++ d1.take(8) ++ d0.drop(12)))
      },
      testM("WriteEntry.freeRanges - free, used overlaps used") {
        for {
          (b0, d0) <- randomBuf(16)
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16)
          (b1, d1) <- randomBuf(8)
          _        <- entry.write(b1, 4, 8)
          _        <- entry.write(b1, 8, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(0, 4)))) &&
          assert(resData)(equalTo(d0.take(4) ++ d1.take(4) ++ d1.take(8)))
      }
    )

  private def randomBuf(size: Int): Task[(ByteBuffer, Chunk[Byte])] =
    for {
      data <- ZIO(Random.nextBytes(size)).map(Chunk.fromArray)
      buf  <- Buffer.byte(data)
      _    <- buf.flip
    } yield (buf, data)

  private def bufToChunk(buf: ByteBuffer): Task[Chunk[Byte]] =
    for {
      _   <- buf.position(0)
      _   <- buf.limit(buf.capacity)
      res <- buf.getChunk()
    } yield res

}
