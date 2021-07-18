package torr.misc

import scala.annotation.tailrec

object URLEncode {

  def encode(data: Array[Byte]): String = {

    @tailrec
    def go(i: Int, acc: StringBuilder): String = {
      if (i >= data.length) {
        acc.toString()
      } else {
        if (
          (data(i) >= 'a' && data(i) <= 'z') ||
          (data(i) >= 'A' && data(i) <= 'Z') ||
          (data(i) >= '0' && data(i) <= '9') ||
          Array('-', '_', '.', '~').contains(data(i).toChar)
        ) {
          acc.append(data(i).toChar)
        } else {
          acc.append('%')
          acc.append("%02X".format(data(i)))
        }
        go(i + 1, acc)
      }
    }

    go(0, new StringBuilder)
  }

}
