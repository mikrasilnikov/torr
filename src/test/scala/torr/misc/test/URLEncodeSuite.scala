package torr.misc.test

import torr.misc.URLEncode
import zio.test._
import zio.test.Assertion._

object URLEncodeSuite extends DefaultRunnableSpec {
  def spec =
    suite("URLEncodeSuite")(
      //
      test("empty value") {
        assert(URLEncode.encode(Array[Byte]()))(equalTo(""))
      },
      //
      test("unreserved") {
        val chars = ('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9') ++ Seq('-', '_', '.', '~')
        assert(URLEncode.encode(chars.map(_.toByte).toArray))(equalTo(chars.mkString))
      },
      //
      test("other") {
        // 64 42
        val chars = "@*"
        assert(URLEncode.encode(chars.map(_.toByte).toArray))(equalTo("%40%2A"))
      }
    )
}
