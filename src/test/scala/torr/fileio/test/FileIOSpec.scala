package torr.fileio.test

import torr.channels._
import torr.fileio.Actor
import torr.fileio.Actor.{File, State}
import zio.test._
import zio.test.Assertion._

object FileIOSpec extends DefaultRunnableSpec {
  def spec =
    suite("FileIOSuite")(
      //
      testM("Search by offset - single file 1") {
        for {
          channel <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb")
          file     = File(0, 20, channel)
          state    = State(10, Vector(file))
          (i, o)   = Actor.fileIndexOffset(0, 0, state)
        } yield assert(i, o)(equalTo(0, 0L))
      },
      //
      testM("Search by offset - single file 2") {
        for {
          channel <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb")
          file     = File(0, 20, channel)
          state    = State(10, Vector(file))
          (i, o)   = Actor.fileIndexOffset(1, 5, state)
        } yield assert(i, o)(equalTo(0, 15L))
      },
      //
      testM("Search by offset - multiple files x00") {
        for {
          f1    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(0, 20, c))
          f2    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(20, 20, c))
          f3    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(40, 20, c))
          state  = State(10, Vector(f1, f2, f3))
          (i, o) = Actor.fileIndexOffset(0, 5, state)
        } yield assert(i, o)(equalTo(0, 5L))
      },
      //
      testM("Search by offset - multiple files 0x0") {
        for {
          f1    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(0, 20, c))
          f2    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(20, 20, c))
          f3    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(40, 20, c))
          state  = State(10, Vector(f1, f2, f3))
          (i, o) = Actor.fileIndexOffset(2, 3, state)
        } yield assert(i, o)(equalTo(1, 3L))
      },
      //
      testM("Search by offset - multiple files 00x") {
        for {
          f1    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(0, 20, c))
          f2    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(20, 20, c))
          f3    <- InMemoryChannel.make("aaaaaaaaaabbbbbbbbbb").map(c => File(40, 20, c))
          state  = State(10, Vector(f1, f2, f3))
          (i, o) = Actor.fileIndexOffset(5, 1, state)
        } yield assert(i, o)(equalTo(2, 11L))
      }
    )
}
