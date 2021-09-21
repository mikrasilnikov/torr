package torr.dispatcher

import java.security.MessageDigest

/**
  * `Peer procedure` is expected to update `digest` while downloading a piece. When `offset` becomes equal to `length`,
  * `digest` should contain SHA-1 hash of the downloaded piece.
  */
final case class DownloadJob(
    pieceId: PieceId,
    offset: Int,
    length: Int
) {
  val digest: MessageDigest = MessageDigest.getInstance("SHA-1")
  def isCompleted: Boolean  = offset >= length
}
