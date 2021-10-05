package torr.consoleui

import torr.dispatcher.Actor.{State => DispatcherState}

import scala.annotation.tailrec

object TorrentProgressBar {

  def renderProgressBar(state: DispatcherState, label: String, numCells: Int): String = {
    val sb = new StringBuilder

    sb.append(label)
    sb.append(" [")
    renderProgressCells(state, numCells, sb)
    sb.append("] ")

    val numPieces    = state.metaInfo.pieceHashes.size
    val numCompleted = state.metaInfo.pieceHashes.indices.count(state.localHave)
    val progress     = numCompleted * 100 / numPieces
    val progressStr  = f"$progress%3d"
    sb.append(" " * (progressStr.length - 3))
    sb.append(progressStr)
    sb.append("%")

    sb.result()
  }

  private def renderProgressCells(state: DispatcherState, numCells: Int, acc: StringBuilder): Unit = {
    val numPieces = state.metaInfo.pieceHashes.size

    @tailrec
    def loop(cell: Int): Unit = {
      if (cell >= numCells) ()
      else {
        val rangeFrom = cell / numCells.toDouble
        val rangeTo   = (cell + 1) / numCells.toDouble
        val pieceFrom = math.floor(numPieces * rangeFrom).toInt
        val pieceTo   = math.ceil(numPieces * rangeTo).toInt

        val cellIsCompleted = (pieceFrom until pieceTo).forall(i => state.localHave(i))

        if (cellIsCompleted) acc.append("=")
        else acc.append(" ")
        loop(cell + 1)
      }
    }

    loop(0)
  }
}
