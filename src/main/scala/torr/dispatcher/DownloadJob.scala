package torr.dispatcher

import zio._
import zio.nio.core.ByteBuffer
import java.security.MessageDigest

/**
  * `Peer routine` is expected to update `digest` while downloading a piece by calling `hashBlock`.
  * When `offset` becomes equal to `length`, `digest` should contain SHA-1 hash of the downloaded piece.
  */
final case class DownloadJob(
    pieceId: PieceId,
    pieceLength: Int,
    private var hashOffset: Int = 0
) { self =>
  def isCompleted: Boolean  = hashOffset >= pieceLength
  val digest: MessageDigest = MessageDigest.getInstance("SHA-1")

  def hashBlock(dataOffset: Int, data: ByteBuffer): Task[Unit] = {
    if (dataOffset != hashOffset) {
      ZIO.fail(new IllegalArgumentException(
        s"Blocks must be hashed in order. Current offset = $hashOffset, receivedOffset = $dataOffset"
      ))
    } else {
      for {
        size <- data.remaining
        _    <- data.mark
        _    <- data.withJavaBuffer(jBuf => ZIO(digest.update(jBuf)))
        _    <- data.reset
        _     = hashOffset += size
      } yield ()
    }
  }

  def getOffset: Int = hashOffset

  override def equals(obj: Any): Boolean =
    obj match {
      case that @ DownloadJob(_, _, _) => that.pieceId == self.pieceId
      case _                           => false
    }

  override def hashCode(): Int = pieceId.hashCode()
}
