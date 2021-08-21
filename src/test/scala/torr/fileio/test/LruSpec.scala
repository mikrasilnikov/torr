package torr.fileio.test

import torr.fileio.Actor.LRUEntry
import zio._
import zio.test._
import zio.test.Assertion._
import torr.fileio.{Actor, EntryAddr, ReadEntry}
import zio.nio.core.Buffer

object LruSpec extends DefaultRunnableSpec {
  override def spec =
    suite("LruSpec")(
      //
      testM("allocateOrFreeCacheBuf - empty queue") {
        val rnd = new java.util.Random(42)
        for {
          state  <- Helpers.createState(rnd, 64 :: Nil, 32, 16, 1)
          actual <- Actor.allocateOrFreeCacheBuf(state)
        } yield assert(actual.capacity)(equalTo(16))
      },
      //
      testM("allocateOrFreeCacheBuf - (valid, valid)") {
        val rnd = new java.util.Random(42)
        for {
          state <- Helpers.createState(rnd, 64 :: Nil, 32, 16, 2)

          e1 <- createReadEntry(state, EntryAddr(0, 0))
          _   = state.cacheState.cacheEntries.put(e1.addr, e1)
          _   = e1.relevance = 100
          _   = state.cacheState.relevances.enqueue(LRUEntry(100, e1))

          e2 <- createReadEntry(state, EntryAddr(0, 1))
          _   = state.cacheState.cacheEntries.put(e2.addr, e2)
          _   = e2.relevance = 200
          _   = state.cacheState.relevances.enqueue(LRUEntry(200, e2))

          actual <- Actor.allocateOrFreeCacheBuf(state)

        } yield assert(actual)(equalTo(e1.data)) &&
          assert(1)(equalTo(state.cacheState.relevances.size))
      },
      //
      testM("allocateOrFreeCacheBuf - (terminated, valid)") {
        val rnd = new java.util.Random(42)
        for {
          state <- Helpers.createState(rnd, 64 :: Nil, 32, 16, 2)

          e1 <- createReadEntry(state, EntryAddr(0, 0))
          _   = state.cacheState.cacheEntries.put(e1.addr, e1)
          _   = e1.relevance = 100
          _   = e1.invalid = true
          _   = state.cacheState.relevances.enqueue(LRUEntry(100, e1))

          e2 <- createReadEntry(state, EntryAddr(0, 1))
          _   = state.cacheState.cacheEntries.put(e2.addr, e2)
          _   = e2.relevance = 200
          _   = state.cacheState.relevances.enqueue(LRUEntry(200, e2))

          actual <- Actor.allocateOrFreeCacheBuf(state)

        } yield assert(actual)(equalTo(e2.data)) &&
          assert(0)(equalTo(state.cacheState.relevances.size))
      },
      //
      testM("allocateOrFreeCacheBuf - (updated, valid)") {
        val rnd = new java.util.Random(42)
        for {
          state <- Helpers.createState(rnd, 64 :: Nil, 32, 16, 2)

          e1 <- createReadEntry(state, EntryAddr(0, 0))
          _   = state.cacheState.cacheEntries.put(e1.addr, e1)
          _   = e1.relevance = 101
          _   = state.cacheState.relevances.enqueue(LRUEntry(100, e1))

          e2 <- createReadEntry(state, EntryAddr(0, 1))
          _   = state.cacheState.cacheEntries.put(e2.addr, e2)
          _   = e2.relevance = 200
          _   = state.cacheState.relevances.enqueue(LRUEntry(200, e2))

          actual <- Actor.allocateOrFreeCacheBuf(state)

        } yield assert(actual)(equalTo(e2.data)) &&
          assert(0)(equalTo(state.cacheState.relevances.size))
      }
    )

  def createReadEntry(state: Actor.State, addr: EntryAddr): Task[ReadEntry] = {
    for {
      buf  <- Buffer.byte(state.cacheState.entrySize)
      size <- Actor.readAddr(state, addr, buf)
    } yield ReadEntry(addr, buf, size)
  }
}
