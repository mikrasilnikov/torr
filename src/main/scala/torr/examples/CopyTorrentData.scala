package torr.examples

import torr.actorsystem._
import torr.channels.AsyncFileChannel
import torr.directbuffers.{DirectBufferPool, FixedBufferPool}
import torr.fileio.Actor._
import torr.fileio._
import torr.metainfo._
import zio.actors.ActorRef
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.logging.slf4j.Slf4jLogger
import zio.nio.core.channels.AsynchronousFileChannel
import zio.nio.core.file.Path
import zio.nio.core._
import zio.nio.file.Files
import zio._
import zio.duration.durationInt
import zio.magic.ZioProvideMagicOps

import java.nio.file._

object CopyTorrentData extends App {

  val metaInfoFile     =
    "d:\\Torrents\\!torrent\\Ragdoll_Masters v3.1.torrent"
  val srcDirectoryName = "c:\\!temp\\CopyTest\\"
  val dstDirectoryName = "c:\\!temp\\CopyTest1\\"

  val blockSize: Int = 16 * 1024

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val srcFileIO = FileIOLive.make(metaInfoFile, srcDirectoryName, allocateFiles = false, actorName = "FileIO1").build
    val dstFileIO = FileIOLive.make(metaInfoFile, dstDirectoryName, allocateFiles = false, actorName = "FileIO2").build

    val effect = for {
      _ <- (srcFileIO <*> dstFileIO).use {
             case (src, dst) =>
               for {
                 metaInfo <- src.get.metaInfo
                 res      <- copyRandom(metaInfo.pieceSize, metaInfo.torrentSize, src.get, dst.get)
                 //res      <- copySequential(0, 0, metaInfo.pieces.length, metaInfo.pieceSize, torrentSize, src, dst)
               } yield res
           }
    } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      Slf4jLogger.make((_, message) => message),
      FixedBufferPool.make(1, 32 * 1024)
    ).exitCode
  }

  def copySequential(
      piece: Int,
      offset: Int,
      pieces: Int,
      pieceLength: Int,
      torrentLength: Long,
      src: FileIO.Service,
      dst: FileIO.Service
  ): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] = {

    val effectiveOffset = piece.toLong * pieceLength + offset

    //noinspection SimplifyUnlessInspection
    if (effectiveOffset >= torrentLength) {
      dst.flush
    } else {
      val readAmount = math.min(blockSize, torrentLength - effectiveOffset).toInt
      for {
        data <- src.fetch(piece, offset, readAmount)
        _    <- dst.store(piece, offset, data)
        _    <- printBuffersStats(effectiveOffset).when(effectiveOffset % (64 * 1024 * 1024) == 0L).fork
        _    <- (offset + readAmount) match {
                  case x if x == pieceLength =>
                    copySequential(piece + 1, 0, pieces, pieceLength, torrentLength, src, dst)
                  case _                     =>
                    copySequential(piece, offset + readAmount, pieces, pieceLength, torrentLength, src, dst)
                }
      } yield ()
    }
  }

  def copyRandom(
      pieceSize: Int,
      torrentLength: Long,
      src: FileIO.Service,
      dst: FileIO.Service
  ): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] = {
    val rnd      = new java.util.Random(43)
    val bufCount = (torrentLength / blockSize).toInt
    val bufOrder = (0 to bufCount).sortBy(_ => rnd.nextInt()).toList

    def copy(
        bufNums: List[Int],
        bufsWritten: Int = 0
    ): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] =
      bufNums match {
        case Nil     => ZIO.unit
        case n :: ns =>
          val offset      = n.toLong * blockSize
          val piece       = (offset / pieceSize).toInt
          val pieceOffset = (offset % pieceSize).toInt
          val amount      = math.min(blockSize, torrentLength - offset).toInt

          for {
            data <- src.fetch(piece, pieceOffset, amount)
            _    <- dst.store(piece, pieceOffset, data)
            _    <- putStrLn(s"$bufsWritten of $bufCount completed").when(bufsWritten % 1000 == 0)
            _    <- copy(ns, bufsWritten + 1)
          } yield ()
      }

    for {
      _ <- copy(bufOrder)
      _ <- dst.flush
    } yield ()
  }

  def printBuffersStats(position: Long): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] =
    for {
      avail <- DirectBufferPool.numAvailable
      _     <- putStrLn(s"${position / 1024 / 1024} - $avail")
    } yield ()
}
