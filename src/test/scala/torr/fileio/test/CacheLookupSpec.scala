package torr.fileio.test

import torr.fileio.{Actor, EntryAddr, IntRange, ReadEntry}
import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec
import torr.fileio.Actor._
import zio.nio.core.Buffer
import torr.fileio.test.Helpers._
import scala.util.Random

object CacheLookupSpec extends DefaultRunnableSpec {
  override def spec =
    suite("CacheLookupSpec")(
      //
      testM("1 file, 1 entry, 1 miss") {
        val rnd = new java.util.Random(42)
        for {
          state   <- createState(rnd, 32 :: Nil, 32, 32, 1)
          lookup   = Actor.cacheLookup(state, 0, 32)
          expected = Miss(EntryAddr(0, 0), IntRange(0, 32))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 2 entries, 2 misses") {
        val rnd = new java.util.Random(42)
        for {
          state   <- createState(rnd, 32 :: Nil, 32, 16, 2)
          lookup   = Actor.cacheLookup(state, 0, 32)
          expected = Miss(EntryAddr(0, 0), IntRange(0, 16))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 1 entry, 1 unaligned miss") {
        val rnd = new java.util.Random(42)
        for {
          state   <- createState(rnd, 32 :: Nil, 32, 32, 2)
          lookup   = Actor.cacheLookup(state, 8, 16)
          expected = Miss(EntryAddr(0, 0), IntRange(8, 24))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("2 files, overlapping miss") {
        val rnd = new java.util.Random(42)
        for {
          state   <- createState(rnd, 32 :: 32 :: Nil, 32, 32, 2)
          lookup   = Actor.cacheLookup(state, 24, 16)
          expected = Miss(EntryAddr(0, 0), IntRange(24, 32))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("3 files, overlapping miss") {
        val rnd = new java.util.Random(42)
        for {
          state   <- createState(rnd, 32 :: 32 :: 32 :: Nil, 32, 32, 2)
          lookup   = Actor.cacheLookup(state, 32 + 24, 16)
          expected = Miss(EntryAddr(1, 0), IntRange(24, 32))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 1 entry, 1 hit") {
        val rnd = new java.util.Random(42)
        for {
          state    <- createState(rnd, 32 :: Nil, 32, 32, 1)
          entryBuf <- Buffer.byte(32)
          entry     = ReadEntry(EntryAddr(0, 0), entryBuf, 32)
          _         = state.cache.addrToEntry.put(entry.addr, entry)
          lookup    = Actor.cacheLookup(state, 0, 32)
          expected  = Hit(entry, IntRange(0, 32))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 2 entries, 2 hits") {
        val rnd = new java.util.Random(42)
        for {
          state <- createState(rnd, 32 :: Nil, 32, 16, 2)

          b1 <- Buffer.byte(32)
          e1  = ReadEntry(EntryAddr(0, 0), b1, 32)
          _   = state.cache.addrToEntry.put(e1.addr, e1)

          b2 <- Buffer.byte(32)
          e2  = ReadEntry(EntryAddr(0, 1), b2, 32)
          _   = state.cache.addrToEntry.put(e2.addr, e2)

          lookup   = Actor.cacheLookup(state, 0, 32)
          expected = Hit(e1, IntRange(0, 16))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("1 file, 1 entry, 1 unaligned hit") {
        val rnd = new java.util.Random(42)
        for {
          state <- createState(rnd, 32 :: Nil, 32, 32, 2)

          b1 <- Buffer.byte(32)
          e1  = ReadEntry(EntryAddr(0, 0), b1, 32)
          _   = state.cache.addrToEntry.put(e1.addr, e1)

          lookup   = Actor.cacheLookup(state, 8, 16)
          expected = Hit(e1, IntRange(8, 24))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("2 files, overlapping hit - 1") {
        val rnd = new java.util.Random(42)
        for {
          state <- createState(rnd, 32 :: 32 :: Nil, 32, 32, 2)

          b1 <- Buffer.byte(32)
          e1  = ReadEntry(EntryAddr(0, 0), b1, 32)
          _   = state.cache.addrToEntry.put(e1.addr, e1)

          b2 <- Buffer.byte(32)
          e2  = ReadEntry(EntryAddr(1, 0), b2, 32)
          _   = state.cache.addrToEntry.put(e2.addr, e2)

          lookup   = Actor.cacheLookup(state, 24, 16)
          expected = Hit(e1, IntRange(24, 32))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("2 files, overlapping hit - 2") {
        val rnd = new java.util.Random(42)
        for {
          state <- createState(rnd, 24 :: 24 :: Nil, 16, 16, 2)

          b1 <- Buffer.byte(16)
          e1  = ReadEntry(EntryAddr(0, 1), b1, 8)
          _   = state.cache.addrToEntry.put(e1.addr, e1)

          b2 <- Buffer.byte(16)
          e2  = ReadEntry(EntryAddr(1, 0), b2, 16)
          _   = state.cache.addrToEntry.put(e2.addr, e2)

          lookup   = Actor.cacheLookup(state, 16, 16)
          expected = Hit(e1, IntRange(0, 8))
        } yield assert(lookup)(equalTo(expected))
      },
      //
      testM("3 files, overlapping hit") {
        val rnd = new java.util.Random(42)
        for {
          state <- createState(rnd, 32 :: 32 :: 32 :: Nil, 32, 32, 2)

          b1 <- Buffer.byte(32)
          e1  = ReadEntry(EntryAddr(1, 0), b1, 32)
          _   = state.cache.addrToEntry.put(e1.addr, e1)

          b2 <- Buffer.byte(32)
          e2  = ReadEntry(EntryAddr(2, 0), b2, 32)
          _   = state.cache.addrToEntry.put(e2.addr, e2)

          lookup   = Actor.cacheLookup(state, 32 + 24, 16)
          expected = Hit(e1, IntRange(24, 32))
        } yield assert(lookup)(equalTo(expected))
      }
    )

}
