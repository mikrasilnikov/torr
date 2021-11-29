package torr.consoleui.test

import torr.consoleui.SimpleProgressBar
import zio._
import zio.test._
import zio.test.Assertion._

object SimpleProgressBarSuite extends DefaultRunnableSpec {
  override def spec =
    suite("SimpleProgressBarSuite")(
      //
      test("empty progress bar") {
        val expected = "Test 123 [          ]   0%"
        val actual   = SimpleProgressBar.render("Test 123", 10, 0)
        assert(actual)(equalTo(expected))
      },
      //
      test("completed progress bar") {
        val expected = "Test 123 [===========] 100%"
        val actual   = SimpleProgressBar.render("Test 123", 11, 100)
        assert(actual)(equalTo(expected))
      },
      //
      test("29%") {
        val expected = "Test 123 [==        ]  29%"
        val actual   = SimpleProgressBar.render("Test 123", 10, 29)
        assert(actual)(equalTo(expected))
      },
      //
      test("99%") {
        val expected = "Test 123 [========= ]  99%"
        val actual   = SimpleProgressBar.render("Test 123", 10, 99)
        assert(actual)(equalTo(expected))
      }
    )
}
