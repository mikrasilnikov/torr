package torr.fileio.test

import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.channels.InMemoryChannel
import torr.directbuffers.{DirectBufferPool, DirectBufferPoolLive}
import torr.directbuffers.test.DirectBuffersSpec
import zio._
import zio.test._
import zio.test.Assertion._
import zio.test.TestAspect.sequential
import torr.fileio.Actor.{GetState, Store}
import torr.fileio.{Actor, FileIOLive}
import zio.actors.ActorRef
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.slf4j.Slf4jLogger
import zio.test.environment.{Live, TestClock}

import java.util.Random

object WriteSpec extends DefaultRunnableSpec {
  override def spec =
    suite("WriteSpec")(
      //
      testM("Single write") {
        val rnd = new Random(42)

        val effect = createActorManaged(rnd, 32 :: Nil, 32, 16, 2)
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
      testM("Two sequential writes") {
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
                //_      <- TestClock.adjust(30.seconds)
                state1 <- (actor ? GetState)                 //.debug("3")

                bufsAvailable1 <- DirectBufferPool.numAvailable

                _       <- TestClock.adjust(30.seconds)
                expected = data0 ++ data1 ++ files0(0).drop(16)
                actual  <- state1.files(0).channel.asInstanceOf[InMemoryChannel].getData
              } yield assert(actual)(equalTo(expected)) &&
                assert(bufsAvailable0)(equalTo(0)) &&
                assert(bufsAvailable1)(equalTo(2))
          }

        val env = createEnv(2, 8)
        effect.provideLayer(env)
      }
    ) @@ sequential

  def createEnv(directBuffersNum: Int, directBufferSize: Int) = {
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
