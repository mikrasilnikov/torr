package torr.dispatcher

import zio._
import zio.clock.Clock
import zio.nio.core.ByteBuffer
import java.security.MessageDigest
import torr.directbuffers.DirectBufferPool
import torr.fileio.FileIO
import torr.metainfo.MetaInfo

object Hasher {

  def hashPiece(
      metaInfo: MetaInfo,
      piece: Int
  ): RIO[FileIO with DirectBufferPool with Clock, Chunk[Byte]] = {
    for {
      queue      <- Queue.bounded[Option[Chunk[ByteBuffer]]](128)
      digest      = MessageDigest.getInstance("SHA-1")
      torrentSize = metaInfo.entries.map(_.size).sum
      pieceSize   = math.min(metaInfo.pieceSize.toLong, torrentSize - piece.toLong * metaInfo.pieceSize)
      readFiber  <- readPieceToQueue(piece, pieceSize.toInt, queue).fork
      res        <- hashQueue(queue, digest)
      _          <- readFiber.await
    } yield res
  }

  //noinspection SimplifyUnlessInspection
  private def readPieceToQueue(
      piece: Int,
      pieceSize: Int,
      queue: Queue[Option[Chunk[ByteBuffer]]],
      offset: Int = 0
  ): ZIO[FileIO with DirectBufferPool, Throwable, Unit] = {
    val remaining: Int = pieceSize - offset
    if (remaining <= 0) queue.offer(None).unit
    else {
      val bytesToRead = math.min(torr.fileio.DefaultBufferSize, remaining)
      for {
        data      <- FileIO.fetch(piece, offset, bytesToRead)
        bytesRead <- ZIO.foldLeft(data)(0) { case (acc, buf) => buf.remaining.map(acc + _) }
        _         <- queue.offer(Some(data))
        _         <- readPieceToQueue(piece, pieceSize, queue, offset + bytesRead)
      } yield ()
    }
  }

  def hashQueue(
      queue: Queue[Option[Chunk[ByteBuffer]]],
      digest: MessageDigest
  ): RIO[DirectBufferPool, Chunk[Byte]] = {
    queue.take.flatMap {
      case None => ZIO.succeed(Chunk.fromArray(digest.digest()))

      case Some(chunk) =>
        ZIO.foreach_(chunk)(buf => buf.withJavaBuffer(jBuf => ZIO(digest.update(jBuf)))) *>
          ZIO.foreach_(chunk)(DirectBufferPool.free) *>
          hashQueue(queue, digest)
    }
  }
}
