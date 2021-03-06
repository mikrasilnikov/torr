package torr.misc

import zio._
import scala.annotation.tailrec

object URLEncode {

  private val extra = Array[Byte]('-', '_', '.', '~')

  def encode(data: Chunk[Byte]): String = {

    @tailrec
    def loop(i: Int, acc: StringBuilder): String = {
      if (i >= data.length) {
        acc.toString()
      } else {
        if (
          (data(i) >= 'a' && data(i) <= 'z') ||
          (data(i) >= 'A' && data(i) <= 'Z') ||
          (data(i) >= '0' && data(i) <= '9') ||
          extra.contains(data(i))
        ) {
          acc.append(data(i).toChar)
        } else {
          acc.append('%')
          acc.append("%02X".format(data(i)))
        }
        loop(i + 1, acc)
      }
    }

    loop(0, new StringBuilder)
  }

}
