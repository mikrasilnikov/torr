package torr.misc.test

import torr.misc.Traverse
import zio.test._
import zio.test.Assertion._

//noinspection Specs2Matchers
object TraverseSuite extends DefaultRunnableSpec {
  def spec =
    suite("TraverseSuite")(
      //
      test("empty list") {
        assert(Traverse.sequence(List()))(equalTo(Some(List())))
      },
      //
      test("singleton list") {
        assert(Traverse.sequence(List(Some(1))))(equalTo(Some(List(1))))
      },
      //
      test("nonempty list") {
        assert(Traverse.sequence(List(Some(1), Some(2))))(equalTo(Some(List(1, 2))))
      },
      //
      test("with none") {
        assert(Traverse.sequence(List(Some(1), None)))(equalTo(None))
      }
    )
}
