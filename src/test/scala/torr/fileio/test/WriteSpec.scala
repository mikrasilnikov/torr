package torr.fileio.test

import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.channels.InMemoryChannel
import torr.directbuffers.{DirectBufferPool, DirectBufferPoolLive}
import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.sequential
import torr.fileio.Actor.{GetState, Store}
import torr.fileio.test.ReadSpec.channelToChunk
import torr.fileio.{Actor, EntryAddr, FileIOLive, ReadEntry}
import zio.actors.ActorRef
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.test.environment.{Live, TestClock}

import java.util.Random

object WriteSpec extends DefaultRunnableSpec {
  override def spec =
    suite("WriteSpec")(
      //
      testM("Single write - with write out") {
        val rnd = new Random(42)

        val effect =
          createActorManaged(rnd, 32 :: Nil, 32, 16, 2)
            .use {
              case (actor, files0) =>
                for {
                  (_, data0)    <- Helpers.randomBuf(rnd, 8)
                  buf           <- DirectBufferPool.allocate
                  _             <- buf.putChunk(data0) *> buf.flip
                  _             <- actor ! Store(0, 0, Chunk(buf))
                  state1        <- actor ? GetState
                  bufsAvailable <- DirectBufferPool.numAvailable
                  _             <- TestClock.adjust(30.seconds)
                  expected       = data0 ++ files0(0).drop(8)
                  actual        <- state1.files(0).channel.asInstanceOf[InMemoryChannel].getData
                } yield assert(actual)(equalTo(expected)) && assert(bufsAvailable)(equalTo(1))
            }

        val env = createEnv(1, 8)
        effect.provideLayer(env)
      },
      //
      testM("Single write - before write out") {
        val rnd = new Random(42)

        val effect =
          createActorManaged(rnd, 32 :: Nil, 32, 16, 2)
            .use {
              case (actor, files0) =>
                for {
                  (_, data0)    <- Helpers.randomBuf(rnd, 8)
                  buf           <- DirectBufferPool.allocate
                  _             <- buf.putChunk(data0) *> buf.flip
                  _             <- actor ! Store(0, 0, Chunk(buf))
                  state1        <- actor ? GetState
                  bufsAvailable <- DirectBufferPool.numAvailable
                  _             <- TestClock.adjust(29.seconds)
                  expected       = files0(0)
                  actual        <- state1.files(0).channel.asInstanceOf[InMemoryChannel].getData
                } yield assert(actual)(equalTo(expected)) && assert(bufsAvailable)(equalTo(1))
            }

        val env = createEnv(1, 8)
        effect.provideLayer(env)
      },
      //
      testM("Two sequential writes - with immediate write out") {
        val rnd = new Random(42)

        val effect = createActorManaged(rnd, 32 :: Nil, 32, 16, 2)
          .use {
            case (actor, files0) =>
              for {
                (_, data0) <- Helpers.randomBuf(rnd, 8)
                (_, data1) <- Helpers.randomBuf(rnd, 8)
                buf0       <- DirectBufferPool.allocate
                buf1       <- DirectBufferPool.allocate
                _          <- buf0.putChunk(data0) *> buf0.flip
                _          <- buf1.putChunk(data1) *> buf1.flip

                bufsAvailable0 <- DirectBufferPool.numAvailable

                _      <- (actor ! Store(0, 0, Chunk(buf0))) //.debug("1")
                _      <- (actor ! Store(0, 8, Chunk(buf1))) //.debug("2")
                state1 <- (actor ? GetState)                 //.debug("3")

                bufsAvailable1 <- DirectBufferPool.numAvailable

                //_ <- TestClock.adjust(29.seconds) // Not enough time to trigger write out event

                expected = data0 ++ data1 ++ files0(0).drop(16)
                actual  <- state1.files(0).channel.asInstanceOf[InMemoryChannel].getData

              } yield assert(actual)(equalTo(expected)) &&
                assert(bufsAvailable0)(equalTo(0)) &&
                assert(bufsAvailable1)(equalTo(2))
          }

        val env = createEnv(2, 8)
        effect.provideLayer(env)
      },
      //
      testM("read - hits dirty WriteEntry") {
        val rnd    = new Random(42)
        val effect = createActorManaged(rnd, 32 :: Nil, 32, 16, 2)
          .use {
            case (actor, files0) =>
              for {
                state      <- actor ? GetState
                writeEntry <- Actor.makeWriteEntry(actor, state, EntryAddr(0, 0))
                (b0, d0)   <- Helpers.randomBuf(rnd, 8)
                _          <- writeEntry.write(b0, 4, 8)

                readResult <- Actor.read(state, 0, 16)
                actual     <- ZIO.foreach(readResult)(b => b.getChunk())

                expected = files0.head.take(4) ++ d0 ++ files0.head.drop(12).take(4)

              } yield assert(state.cache.cacheHits)(equalTo(2L)) &&
                assert(actual.flatten)(equalTo(expected))
          }

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          DirectBufferPoolLive.make(2, 8)
        )
      }
    ) @@ sequential

  def createEnv(
      directBuffersNum: Int,
      directBufferSize: Int
  ) = {
    val actorSystem = ActorSystemLive.make("Test")
    val logging     = Slf4jLogger.make((_, message) => message)
    val clock       = TestClock.default
    val bufferPool  = (actorSystem ++ logging ++ clock) >>> DirectBufferPoolLive.make(directBuffersNum, directBufferSize)
    clock ++ bufferPool ++ actorSystem
  }

  def createActorManaged(
      rnd: java.util.Random,
      sizes: List[Int],
      pieceSize: Int,
      cacheEntrySize: Int,
      cacheEntriesNum: Int
  ): ZManaged[
    ActorSystem with DirectBufferPool with Clock,
    Throwable,
    (ActorRef[Actor.Command], Vector[Chunk[Byte]])
  ] = {
    for {
      state <- Helpers.createState(rnd, sizes, pieceSize, cacheEntrySize, cacheEntriesNum).toManaged_
      data  <- ZIO.foreach(state.files)(f => f.channel.asInstanceOf[InMemoryChannel].getData).toManaged_
      actor <- FileIOLive.createActorManaged("FileIO", state)
    } yield (actor, data)
  }

}
