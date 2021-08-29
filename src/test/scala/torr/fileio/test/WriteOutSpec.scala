package torr.fileio.test

import zio._
import zio.test._
import zio.test.Assertion._
import torr.channels.InMemoryChannel
import torr.fileio.{EntryAddr, WriteEntry}
import torr.fileio.test.Helpers._
import zio.nio.core.Buffer

object WriteOutSpec extends DefaultRunnableSpec {
  override def spec =
    suite("WriteOutSpec")(
      //
      testM("Full entry, one write") {
        val rnd = new java.util.Random(42)
        for {
          state    <- createState(rnd, 32 :: Nil, 32, 32, 1)
          (b0, _)  <- randomBuf(rnd, 32)
          (b1, d1) <- randomBuf(rnd, 32)
          now      <- clock.currentDateTime
          entry     = WriteEntry(EntryAddr(0, 0), b0, 32, now)
          _        <- entry.write(b1, 0, 32)
          _        <- torr.fileio.Actor.synchronizeAndWriteOut(state, entry)
          actual   <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData
        } yield assert(actual)(equalTo(d1))
      },
      //
      testM("Full entry, two writes") {
        val rnd = new java.util.Random(42)
        for {
          state    <- createState(rnd, 32 :: Nil, 32, 32, 1)
          (b0, _)  <- randomBuf(rnd, 32)
          (b1, d1) <- randomBuf(rnd, 16)
          (b2, d2) <- randomBuf(rnd, 16)
          now      <- clock.currentDateTime
          entry     = WriteEntry(EntryAddr(0, 0), b0, 32, now)
          _        <- entry.write(b1, 0, 16)
          _        <- entry.write(b2, 16, 16)
          _        <- torr.fileio.Actor.synchronizeAndWriteOut(state, entry)
          actual   <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData
        } yield assert(actual)(equalTo(d1 ++ d2))
      },
      //
      testM("Out of sync entry, first half used") {
        val rnd = new java.util.Random(42)
        for {
          fd0   <- ZIO(randomChunk(rnd, 8))
          state <- createState(fd0 :: Nil, 8, 8, 1)

          now   <- clock.currentDateTime
          entry <- Buffer.byte(8).map(b => WriteEntry(EntryAddr(0, 0), b, 8, now))

          (b1, d1) <- randomBuf(rnd, 4)

          _      <- entry.write(b1, 0, 4)
          _      <- torr.fileio.Actor.synchronizeAndWriteOut(state, entry)
          actual <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData
        } yield assert(actual)(equalTo(d1 ++ fd0.drop(4)))
      },
      //
      testM("Out of sync entry, last half used") {
        val rnd = new java.util.Random(42)
        for {
          fd0   <- ZIO(randomChunk(rnd, 8))
          state <- createState(fd0 :: Nil, 8, 8, 1)

          now   <- clock.currentDateTime
          entry <- Buffer.byte(8).map(b => WriteEntry(EntryAddr(0, 0), b, 8, now))

          (b1, d1) <- randomBuf(rnd, 4)
          _        <- entry.write(b1, 4, 4)

          _      <- torr.fileio.Actor.synchronizeAndWriteOut(state, entry)
          actual <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData
        } yield assert(actual)(equalTo(fd0.take(4) ++ d1))
      },
      //
      testM("Out of sync entry, middle used") {
        val rnd = new java.util.Random(42)
        for {
          fd0   <- ZIO(randomChunk(rnd, 8))
          state <- createState(fd0 :: Nil, 8, 8, 1)

          now   <- clock.currentDateTime
          entry <- Buffer.byte(8).map(b => WriteEntry(EntryAddr(0, 0), b, 8, now))

          (b1, d1) <- randomBuf(rnd, 4)
          _        <- entry.write(b1, 2, 4)

          _      <- torr.fileio.Actor.synchronizeAndWriteOut(state, entry)
          actual <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData
        } yield assert(actual)(equalTo(fd0.take(2) ++ d1 ++ fd0.drop(6)))
      }
    )
}
