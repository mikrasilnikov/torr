package torr.dispatcher

import torr.dispatcher.WantedPieces.Piece
import scala.collection.mutable

case class WantedPieces(
    pieceRarity: Map[Piece, Long],
    piecesByRarity: mutable.TreeMap[Long, Set[Piece]]
) {
  def tryDequeueExistingIn(that: Set[Piece]): Option[Piece] = ???
  def increaseAvailability(piece: Piece): Unit              = ???
  def increaseAvailability(pieces: Set[Piece]): Unit        = ???
}

object WantedPieces {
  type Piece = Long
}
