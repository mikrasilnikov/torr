package torr.consoleui

object StringExtensions {

  implicit class StringOps(s: String) {
    def padLeft(toLength: Int): String = {
      if (s.length < toLength)
        " " * (toLength - s.length) ++ s
      else s
    }
    def padRight(toLength: Int): String = {
      if (s.length < toLength)
        s ++ " " * (toLength - s.length)
      else s
    }
  }
}
