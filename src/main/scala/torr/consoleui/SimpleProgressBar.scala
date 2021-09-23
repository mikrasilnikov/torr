package torr.consoleui

object SimpleProgressBar {
  def render(label: String, barWidth: Int, progress: Int): String = {
    val sb             = new StringBuilder
    val completedItems = barWidth * progress / 100

    sb.append(label)
    sb.append(" [")
    sb.append("=" * completedItems)
    sb.append(" " * (barWidth - completedItems))
    sb.append("] ")
    val progressStr = f"$progress%3d"
    sb.append(" " * (progressStr.length - 3))
    sb.append(progressStr)
    sb.append("%")
    sb.result()
  }
}
