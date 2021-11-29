package torr.fileio.test

import zio._
import zio.test._
import zio.test.Assertion._
import torr.fileio.{EntryAddr, IntRange, WriteEntry}
import torr.fileio.test.Helpers._
import zio.test.environment.TestClock

import scala.util.Random

object CacheEntrySuite extends DefaultRunnableSpec {
  override def spec =
    suite("CacheEntrySuite")(
      //
      testM("WriteEntry.freeRanges - empty entry") {
        val rnd = new java.util.Random(42)
        for {
          (b0, _) <- randomBuf(rnd, 16)
          now     <- clock.currentDateTime
          entry    = WriteEntry(EntryAddr(0, 0), b0, 16, now)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(0, 16))))
      },
      testM("WriteEntry.freeRanges - first half used") {
        val rnd = new java.util.Random(42)
        for {
          (b0, d0) <- randomBuf(rnd, 16)
          now      <- clock.currentDateTime
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16, now)
          (b1, d1) <- randomBuf(rnd, 8)
          _        <- entry.write(b1, 0, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(8, 16)))) &&
          assert(resData)(equalTo(d1 ++ d0.drop(8)))
      },
      testM("WriteEntry.freeRanges - last half used") {
        val rnd = new java.util.Random(42)
        for {
          (b0, d0) <- randomBuf(rnd, 16)
          now      <- clock.currentDateTime
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16, now)
          (b1, d1) <- randomBuf(rnd, 8)
          _        <- entry.write(b1, 8, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(0, 8)))) &&
          assert(resData)(equalTo(d0.take(8) ++ d1))
      },
      testM("WriteEntry.freeRanges - free, used, free") {
        val rnd = new java.util.Random(42)
        for {
          (b0, d0) <- randomBuf(rnd, 16)
          now      <- clock.currentDateTime
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16, now)
          (b1, d1) <- randomBuf(rnd, 8)
          _        <- entry.write(b1, 4, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(12, 16), IntRange(0, 4)))) &&
          assert(resData)(equalTo(d0.take(4) ++ d1 ++ d0.drop(12)))
      },
      testM("WriteEntry.freeRanges - used, free, used") {
        val rnd = new java.util.Random(42)
        for {
          (b0, d0) <- randomBuf(rnd, 16)
          now      <- clock.currentDateTime
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16, now)
          (b1, d1) <- randomBuf(rnd, 8)
          _        <- entry.write(b1, 0, 4)
          _        <- b1.position(0)
          _        <- entry.write(b1, 12, 4)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(4, 12)))) &&
          assert(resData)(equalTo(d1.take(4) ++ d0.drop(4).take(8) ++ d1.take(4)))
      },
      testM("WriteEntry.freeRanges - used overlaps used, free") {
        val rnd = new java.util.Random(42)
        for {
          (b0, d0) <- randomBuf(rnd, 16)
          now      <- clock.currentDateTime
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16, now)
          (b1, d1) <- randomBuf(rnd, 8)
          _        <- entry.write(b1, 0, 8)
          _        <- b1.position(0)
          _        <- entry.write(b1, 4, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(12, 16)))) &&
          assert(resData)(equalTo(d1.take(4) ++ d1.take(8) ++ d0.drop(12)))
      },
      testM("WriteEntry.freeRanges - free, used overlaps used") {
        val rnd = new java.util.Random(42)
        for {
          (b0, d0) <- randomBuf(rnd, 16)
          now      <- clock.currentDateTime
          entry     = WriteEntry(EntryAddr(0, 0), b0, 16, now)
          (b1, d1) <- randomBuf(rnd, 8)
          _        <- entry.write(b1, 4, 8)
          _        <- b1.position(0)
          _        <- entry.write(b1, 8, 8)
          resData  <- bufToChunk(b0)
        } yield assert(entry.freeRanges)(equalTo(List(IntRange(0, 4)))) &&
          assert(resData)(equalTo(d0.take(4) ++ d1.take(4) ++ d1.take(8)))
      }
    )
}
