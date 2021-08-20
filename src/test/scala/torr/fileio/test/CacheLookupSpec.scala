package torr.fileio.test

import torr.channels.InMemoryChannel
import torr.fileio.{Actor, EntryAddr, IntRange, ReadEntry}
import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import torr.fileio.Actor._
import zio.nio.core.Buffer

import scala.util.Random

object CacheLookupSpec extends DefaultRunnableSpec {
  override def spec =
    suite("CacheLookupSpec")(
      //
      testM("1 file, 1 entry, 1 miss") {
        for {
          state   <- createState(32 :: Nil, 32, 32, 1)
          lookup  <- Actor.cacheLookup(state, 0, 32)
          expected = Chunk(Miss(EntryAddr(0, 0), IntRange(0, 32)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 2 entries, 2 misses") {
        for {
          state   <- createState(32 :: Nil, 32, 16, 2)
          lookup  <- Actor.cacheLookup(state, 0, 32)
          expected = Chunk(Miss(EntryAddr(0, 0), IntRange(0, 16)), Miss(EntryAddr(0, 1), IntRange(0, 16)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 1 entry, 1 unaligned miss") {
        for {
          state   <- createState(32 :: Nil, 32, 32, 2)
          lookup  <- Actor.cacheLookup(state, 8, 16)
          expected = Chunk(Miss(EntryAddr(0, 0), IntRange(8, 24)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("2 files, overlapping miss") {
        for {
          state   <- createState(32 :: 32 :: Nil, 32, 32, 2)
          lookup  <- Actor.cacheLookup(state, 24, 16)
          expected = Chunk(Miss(EntryAddr(0, 0), IntRange(24, 32)), Miss(EntryAddr(1, 0), IntRange(0, 8)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("3 files, overlapping miss") {
        for {
          state   <- createState(32 :: 32 :: 32 :: Nil, 32, 32, 2)
          lookup  <- Actor.cacheLookup(state, 32 + 24, 16)
          expected = Chunk(Miss(EntryAddr(1, 0), IntRange(24, 32)), Miss(EntryAddr(2, 0), IntRange(0, 8)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 1 entry, 1 hit") {
        for {
          state    <- createState(32 :: Nil, 32, 32, 1)
          entryBuf <- Buffer.byte(32)
          entry     = ReadEntry(EntryAddr(0, 0), entryBuf, 32)
          _         = state.cacheState.cacheEntries.put(entry.addr, entry)
          lookup   <- Actor.cacheLookup(state, 0, 32)
          expected  = Chunk(Hit(entry, IntRange(0, 32)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 2 entries, 2 hits") {
        for {
          state <- createState(32 :: Nil, 32, 16, 2)

          b1 <- Buffer.byte(32)
          e1  = ReadEntry(EntryAddr(0, 0), b1, 32)
          _   = state.cacheState.cacheEntries.put(e1.addr, e1)

          b2 <- Buffer.byte(32)
          e2  = ReadEntry(EntryAddr(0, 1), b2, 32)
          _   = state.cacheState.cacheEntries.put(e2.addr, e2)

          lookup  <- Actor.cacheLookup(state, 0, 32)
          expected = Chunk(Hit(e1, IntRange(0, 16)), Hit(e2, IntRange(0, 16)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 1 entry, 1 unaligned hit") {
        for {
          state <- createState(32 :: Nil, 32, 32, 2)

          b1 <- Buffer.byte(32)
          e1  = ReadEntry(EntryAddr(0, 0), b1, 32)
          _   = state.cacheState.cacheEntries.put(e1.addr, e1)

          lookup  <- Actor.cacheLookup(state, 8, 16)
          expected = Chunk(Hit(e1, IntRange(8, 24)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("2 files, overlapping hit") {
        for {
          state <- createState(32 :: 32 :: Nil, 32, 32, 2)

          b1 <- Buffer.byte(32)
          e1  = ReadEntry(EntryAddr(0, 0), b1, 32)
          _   = state.cacheState.cacheEntries.put(e1.addr, e1)

          b2 <- Buffer.byte(32)
          e2  = ReadEntry(EntryAddr(1, 0), b2, 32)
          _   = state.cacheState.cacheEntries.put(e2.addr, e2)

          lookup  <- Actor.cacheLookup(state, 24, 16)
          expected = Chunk(Hit(e1, IntRange(24, 32)), Hit(e2, IntRange(0, 8)))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("3 files, overlapping hit") {
        for {
          state <- createState(32 :: 32 :: 32 :: Nil, 32, 32, 2)

          b1 <- Buffer.byte(32)
          e1  = ReadEntry(EntryAddr(1, 0), b1, 32)
          _   = state.cacheState.cacheEntries.put(e1.addr, e1)

          b2 <- Buffer.byte(32)
          e2  = ReadEntry(EntryAddr(2, 0), b2, 32)
          _   = state.cacheState.cacheEntries.put(e2.addr, e2)

          lookup  <- Actor.cacheLookup(state, 32 + 24, 16)
          expected = Chunk(Hit(e1, IntRange(24, 32)), Hit(e2, IntRange(0, 8)))
        } yield assert(lookup)(equalTo(expected))
      }
    )

  def createState(sizes: List[Int], pieceSize: Int, cacheEntrySize: Int, cacheEntriesNum: Int): Task[State] =
    for {
      files     <- createFiles(sizes)
      cacheState = CacheState(cacheEntrySize, cacheEntriesNum)
    } yield State(pieceSize, sizes.sum, files, cacheState)

  def createFiles(sizes: List[Int], offset: Int = 0, res: Vector[OpenedFile] = Vector.empty): Task[Vector[OpenedFile]] =
    sizes match {
      case Nil           => ZIO.succeed(res)
      case size :: sizes =>
        InMemoryChannel.make(Random.nextBytes(size)).flatMap { channel =>
          createFiles(sizes, offset + size, res :+ OpenedFile(offset, size, channel))
        }
    }
}
