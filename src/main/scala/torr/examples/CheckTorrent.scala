package torr.examples

import zio._
import zio.console._
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import java.time.Duration
import torr.actorsystem.ActorSystemLive
import torr.directbuffers.DirectBufferPoolLive
import torr.dispatcher.Hasher
import torr.fileio.{FileIO, FileIOLive}

object CheckTorrent extends App {
  val metaInfoFile      =
    "d:\\Torrents\\!torrent\\Breaking Bad - Season 1 [BDRip] (Кубик в Кубе).torrent"
  val dataDirectoryName = "c:\\!temp\\CopyTest1\\"
  //val dataDirectoryName = "d:\\Torrents\\Breaking Bad - Season 1 [BDRip] (Кубик в Кубе)\\"

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val effect = for {
      metaInfo     <- FileIO.metaInfo
      startTime    <- clock.currentDateTime
      actualHashes <- ZIO.foreach(metaInfo.pieceHashes.indices.toVector) { piece =>
                        for {
                          hash <- Hasher.hashPiece(metaInfo, piece)
                          _    <- putStrLn(s"Piece $piece completed, ${hash.map(_.formatted("%02x")).mkString}").fork
                        } yield hash
                      }
      endTime      <- clock.currentDateTime
      dataSize      = metaInfo.entries.map(_.size).sum
      duration      = Duration.between(startTime, endTime)
      speed         = dataSize / 1024 / 1024 / duration.toSeconds
      valid         = metaInfo.pieceHashes.indices.forall(i => metaInfo.pieceHashes(i) == actualHashes(i))
      _            <- if (valid) putStrLn("Hashes are valid") else putStrLn("Hashes are invalid")
      _            <- putStrLn(s"Speed was $speed MB/s")
    } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      Slf4jLogger.make((_, message) => message),
      DirectBufferPoolLive.make(32),
      FileIOLive.make(metaInfoFile, dataDirectoryName)
    ).exitCode
  }
}
