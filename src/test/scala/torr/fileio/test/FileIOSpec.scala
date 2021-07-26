package torr.fileio.test

import torr.fileio.Actor.State
import zio.test._
import zio.test.Assertion._

object FileIOSpec extends DefaultRunnableSpec {
  def spec =
    suite("FileIOSpec")(
      //
      test("File search - single file") {
        //val state = State()
        assert()(anything)
      }
    )
}
