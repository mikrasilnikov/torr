package torr

import zio._
import zio.test._
import zio.console._
import zio.clock._
import zio.duration.durationInt
import zio.test.Assertion._
import zio.test.TestAspect.sequential
import zio.test.environment.{TestClock, TestConsole}

object TestClockTest extends DefaultRunnableSpec {
  override def spec =
    suite("TestClockTest")(
      //
      testM("1") {
        val effect = for {
          _      <- putStr("Hello").delay(5.seconds).fork
          _      <- TestClock.adjust(5.seconds)
          actual <- TestConsole.output
        } yield assert(actual)(equalTo(Vector("Hello")))

        effect
      }
    ) @@ sequential
}
