package torr.directbuffers.test

import zio._
import zio.test._
import zio.test.Assertion._
import zio.Fiber.Status._
import zio.clock.Clock
import torr.actorsystem.ActorSystemLive
import torr.directbuffers._
import zio.Exit.Success
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.test.TestAspect.{flaky, sequential}

object DirectBuffersSpec extends DefaultRunnableSpec {

  def spec =
    suite("DirectBuffersSuite")(
      //
      testM("GetNumAvailable - 1 available") {
        val effect = for {
          actual <- DirectBufferPool.numAvailable
        } yield assert(actual)(equalTo(1))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          FixedBufferPool.make(1)
        )
      },
      //
      testM("GetNumAvailable - 0 available") {

        val effect = for {
          _      <- DirectBufferPool.allocate
          actual <- DirectBufferPool.numAvailable
        } yield assert(actual)(equalTo(0))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          FixedBufferPool.make(1)
        )
      },
      //
      testM("GetNumAvailable - starving") {
        val effect = for {
          _      <- DirectBufferPool.allocate
          f      <- DirectBufferPool.allocate.fork
          _      <- f.status.repeatWhile { case Suspended(_, _, _, _, _) => false; case _ => true }
          actual <- DirectBufferPool.numAvailable
          _      <- f.interrupt
        } yield assert(actual)(equalTo(-1))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          FixedBufferPool.make(1)
        )
      },
      //
      testM("Allocate - available") {
        val effect = for {
          buf1       <- DirectBufferPool.allocate
          buf2       <- DirectBufferPool.allocate
          (ix1, ix2) <- buf1.getInt <*> buf2.getInt
        } yield assert((ix1, ix2))(equalTo((1, 2)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          FixedBufferPool.make(2)
        )
      },
      //
      testM("Free - when available") {
        val effect = for {
          buf1   <- DirectBufferPool.allocate
          ix1    <- buf1.getInt
          avail1 <- DirectBufferPool.numAvailable
          _      <- DirectBufferPool.free(buf1)
          avail2 <- DirectBufferPool.numAvailable
        } yield assert((ix1, avail1, avail2))(equalTo((1, 1, 2)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          FixedBufferPool.make(2)
        )
      },
      //
      testM("Free - when starving") {
        val effect = for {
          buf1 <- DirectBufferPool.allocate
          buf2 <- DirectBufferPool.allocate
          f3   <- DirectBufferPool.allocate.fork
          _    <- f3.status.repeatWhile { case Suspended(_, _, _, _, _) => false; case _ => true }
          f4   <- DirectBufferPool.allocate.fork
          _    <- DirectBufferPool.free(buf2)
          _    <- DirectBufferPool.free(buf1)
          res3 <- f3.await.flatMap { case Success(buf) => buf.getInt; case _ => ??? }
          res4 <- f4.await.flatMap { case Success(buf) => buf.getInt; case _ => ??? }
        } yield assert((res3, res4))(equalTo((2, 1)))

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          Slf4jLogger.make((_, message) => message),
          FixedBufferPool.make(2)
        )
      }
    ) @@ sequential
}
