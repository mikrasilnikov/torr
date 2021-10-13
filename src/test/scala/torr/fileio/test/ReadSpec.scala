package torr.fileio.test

import zio._
import zio.clock.Clock
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.sequential
import torr.actorsystem.ActorSystemLive
import torr.channels.{InMemoryChannel, SeekableByteChannel}
import torr.directbuffers.FixedBufferPool
import torr.fileio.{Actor, EntryAddr, ReadEntry}
import zio.logging.Logging
import zio.magic.ZioProvideMagicOps
import zio.test.environment.TestClock

import java.util.Random

object ReadSpec extends DefaultRunnableSpec {
  val env0 = ActorSystemLive.make("Test") ++ Logging.ignore

  override def spec =
    suite("ReadSpec")(
      //
      testM("makeReadEntry") {
        val rnd = new Random(42)
        for {
          state    <- Helpers.createState(rnd, 32 :: Nil, 32, 16, 2)
          e        <- Actor.makeReadEntry(state, EntryAddr(0, 0))
          actual   <- Helpers.bufToChunk(e.data)
          expected <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData.map(_.take(16))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("makeReadEntry - less then entry size because of eof") {
        val rnd = new Random(42)
        for {
          state    <- Helpers.createState(rnd, 24 :: Nil, 16, 16, 2)
          e        <- Actor.makeReadEntry(state, EntryAddr(0, 1))
          actual   <- e.data.flip *> e.data.getChunk()
          expected <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData.map(_.drop(16))
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("read - hit whole read entry") {
        val rnd    = new Random(42)
        val effect = for {
          state      <- Helpers.createState(rnd, 32 :: Nil, 32, 16, 2)
          _          <- Actor.makeReadEntry(state, EntryAddr(0, 0))
          readResult <- Actor.read(state, 0, 16)
          actual     <- ZIO.foreach(readResult)(b => b.getChunk())
          expected   <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData.map(_.take(16))
        } yield assert(state.cache.cacheHits)(equalTo(1L)) &&
          assert(actual)(equalTo(Chunk(expected)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      },
      //
      testM("read - hit part of read entry") {
        val rnd    = new Random(42)
        val effect = for {
          state      <- Helpers.createState(rnd, 32 :: Nil, 32, 16, 2)
          _          <- Actor.makeReadEntry(state, EntryAddr(0, 0))
          readResult <- Actor.read(state, 4, 8)
          actual     <- ZIO.foreach(readResult)(b => b.getChunk())
          expected   <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData.map(_.drop(4).take(8))
        } yield assert(state.cache.cacheHits)(equalTo(1L)) &&
          assert(actual)(equalTo(Chunk(expected)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      },
      //
      testM("read - hit eof") {
        val rnd    = new Random(42)
        val effect = for {
          state      <- Helpers.createState(rnd, 32 :: Nil, 32, 16, 2)
          _          <- Actor.makeReadEntry(state, EntryAddr(0, 1))
          readResult <- Actor.read(state, 24, 8)
          actual     <- ZIO.foreach(readResult)(b => b.getChunk())
          expected   <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData.map(_.drop(24).take(8))
        } yield assert(state.cache.cacheHits)(equalTo(1L)) &&
          assert(actual)(equalTo(Chunk(expected)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      },
      //
      testM("read - hit eof hit") {
        val rnd    = new Random(42)
        val effect =
          for {
            state <- Helpers.createState(rnd, 24 :: 24 :: Nil, 16, 16, 2)

            _ <- Actor.makeReadEntry(state, EntryAddr(0, 1))
            _ <- Actor.makeReadEntry(state, EntryAddr(1, 0))

            readResult <- Actor.read(state, 16, 16)
            actual     <- ZIO.foreach(readResult)(b => b.getChunk())

            expected1 <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData.map(_.drop(16).take(8))
            expected2 <- state.files(1).channel.asInstanceOf[InMemoryChannel].getData.map(_.take(8))

          } yield assert(state.cache.cacheHits)(equalTo(2L)) &&
            assert(actual)(equalTo(Chunk(expected1 ++ expected2)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      },
      //
      testM("read - hit boundary miss, with buf recycling") {
        val rnd    = new Random(42)
        val effect =
          for {
            state <- Helpers.createState(rnd, 24 :: 24 :: Nil, 16, 16, 2)

            _ <- Actor.makeReadEntry(state, EntryAddr(0, 1))
            _ <- Actor.makeReadEntry(state, EntryAddr(1, 1))

            readResult <- Actor.read(state, 16, 16)
            actual     <- ZIO.foreach(readResult)(b => b.getChunk())

            expected1 <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData.map(_.drop(16).take(8))
            expected2 <- state.files(1).channel.asInstanceOf[InMemoryChannel].getData.map(_.take(8))

          } yield assert(state.cache.cacheHits)(equalTo(1L)) &&
            assert(actual)(equalTo(Chunk(expected1 ++ expected2)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      },
      //
      testM("read - lookup results entry recycling") {

        /*
          1. read(...) produces two lookup results: Miss and Hit(entry0)
          2. When Miss is processed a new ReadEntry (entry1) is created
          3. To create entry1, entry0 is recycled and removed from cache
          4. When Hit(entry0) is processed, entry0 is no longer valid
         */

        val rnd    = new Random(42)
        val effect =
          for {
            state <- Helpers.createState(rnd, 32 :: Nil, 16, 16, 1)

            _ <- Actor.makeReadEntry(state, EntryAddr(0, 1))

            readResult <- Actor.read(state, 8, 16)
            actual     <- ZIO.foreach(readResult)(b => b.getChunk())

            expected <- state.files(0).channel.asInstanceOf[InMemoryChannel].getData.map(_.drop(8).take(16))

          } yield assert(state.cache.cacheHits)(equalTo(0L)) &&
            assert(actual)(equalTo(Chunk(expected)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Logging.ignore,
          FixedBufferPool.make(8)
        )
      }
    ) @@ sequential

  def channelToChunk(channel: SeekableByteChannel): Task[Chunk[Byte]] =
    channel.asInstanceOf[InMemoryChannel].getData
}
