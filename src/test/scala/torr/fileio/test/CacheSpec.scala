package torr.fileio.test

import torr.fileio.{Cache, EntryAddr, ReadEntry}
import zio._
import zio.nio.core.Buffer
import zio.test._
import zio.test.Assertion._

object CacheSpec extends DefaultRunnableSpec {
  override def spec =
    suite("CacheSpec")(
      //
      testM("entryIsValid - valid") {
        Buffer.byte(16).map { buf =>
          val cache = Cache()
          val e     = ReadEntry(EntryAddr(0, 0), buf, 16)
          cache.add(e)
          assert(cache.entryIsValid(e))(isTrue)
        }
      },
      //
      testM("entryIsValid - invalid") {
        (Buffer.byte(16) <*> Buffer.byte(16))
          .map {
            case (buf1, buf2) =>
              val cache = Cache()
              val e1    = ReadEntry(EntryAddr(0, 0), buf1, 16)
              val e2    = ReadEntry(EntryAddr(0, 0), buf2, 16)
              cache.add(e1)
              assert(cache.entryIsValid(e2))(isFalse)
          }
      },
      //
      testM("pullEntryToRecycle - none") {
        Buffer.byte(16).map { buf =>
          val cache = Cache(16, 2)
          val e     = ReadEntry(EntryAddr(0, 0), buf, 16)
          cache.add(e)
          assert(cache.pullEntryToRecycle)(isNone)
        }
      },
      //
      testM("pullEntryToRecycle - some") {
        Buffer.byte(16).map { buf =>
          val cache = Cache(16, 2)
          val e1    = ReadEntry(EntryAddr(0, 0), buf, 16)
          val e2    = ReadEntry(EntryAddr(0, 1), buf, 16)
          cache.add(e1)
          cache.add(e2)

          val expectedCache = Cache(16, 2)
          expectedCache.add(e2)

          assert(cache.pullEntryToRecycle)(isSome(equalTo(e1))) &&
          assert(cache.addrToEntry.toList)(equalTo(List((EntryAddr(0, 1), e2)))) &&
          assert(cache.addrToRelevance.toList)(equalTo(List((EntryAddr(0, 1), 1L)))) &&
          assert(cache.relevanceToAddr.toList)(equalTo(List((1L, EntryAddr(0, 1)))))
        }
      },
      //
      testM("add") {
        Buffer.byte(16).map { buf =>
          val cache = Cache(16, 1)
          val e     = ReadEntry(EntryAddr(0, 1), buf, 16)
          cache.add(e)

          assert(cache.addrToEntry.toList)(equalTo(List((EntryAddr(0, 1), e)))) &&
          assert(cache.addrToRelevance.toList)(equalTo(List((EntryAddr(0, 1), 0L)))) &&
          assert(cache.relevanceToAddr.toList)(equalTo(List((0L, EntryAddr(0, 1))))) &&
          assert(cache.currentRelevance)(equalTo(1L))
        }
      },
      //
      testM("remove") {
        Buffer.byte(16).map { buf =>
          val cache = Cache(16, 1)
          val e     = ReadEntry(EntryAddr(0, 1), buf, 16)
          cache.add(e)
          cache.remove(e)

          assert(cache.addrToEntry.toList)(equalTo(List())) &&
          assert(cache.addrToRelevance.toList)(equalTo(List())) &&
          assert(cache.relevanceToAddr.toList)(equalTo(List())) &&
          assert(cache.currentRelevance)(equalTo(1L))
        }
      },
      //
      testM("markUse") {
        Buffer.byte(16).map { buf =>
          val cache = Cache(16, 1)
          val e     = ReadEntry(EntryAddr(0, 1), buf, 16)
          cache.add(e)

          cache.markUse(e)
          cache.markUse(e)

          assert(cache.addrToEntry.toList)(equalTo(List((EntryAddr(0, 1), e)))) &&
          assert(cache.addrToRelevance.toList)(equalTo(List((EntryAddr(0, 1), 2L)))) &&
          assert(cache.relevanceToAddr.toList)(equalTo(List((2L, EntryAddr(0, 1))))) &&
          assert(cache.currentRelevance)(equalTo(3L))
        }
      }
    )
}
