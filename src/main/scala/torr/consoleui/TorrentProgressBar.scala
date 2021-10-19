package torr.consoleui
import scala.annotation.tailrec

object TorrentProgressBar {

  def renderProgressBar(localHave: Array[Boolean], label: String, numCells: Int): String = {
    val sb = new StringBuilder

    sb.append(label)
    sb.append(" [")
    renderProgressCells(localHave, numCells, sb)
    sb.append("] ")

    val numPieces    = localHave.length
    val numCompleted = localHave.indices.count(localHave)
    val progress     = numCompleted * 100 / numPieces
    val progressStr  = f"$progress%3d"
    sb.append(" " * (progressStr.length - 3))
    sb.append(progressStr)
    sb.append("%")

    sb.result()
  }

  private def renderProgressCells(localHave: Array[Boolean], numCells: Int, acc: StringBuilder): Unit = {
    val numPieces = localHave.length

    @tailrec
    def loop(cell: Int): Unit = {
      if (cell >= numCells) ()
      else {
        val rangeFrom = cell / numCells.toDouble
        val rangeTo   = (cell + 1) / numCells.toDouble
        val pieceFrom = math.floor(numPieces * rangeFrom).toInt
        val pieceTo   = math.ceil(numPieces * rangeTo).toInt

        val cellIsCompleted = (pieceFrom until pieceTo).forall(i => localHave(i))
        val cellIsPartial   = (pieceFrom until pieceTo).exists(i => localHave(i))

        if (cellIsCompleted) acc.append("=")
        else if (cellIsPartial) acc.append("-")
        else acc.append(" ")
        loop(cell + 1)
      }
    }

    loop(0)
  }
}
