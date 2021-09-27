package torr

import torr.actorsystem.ActorSystemLive
import torr.directbuffers.FixedBufferPool
import torr.dispatcher.{Dispatcher, DispatcherLive, PieceId}
import torr.fileio.FileIOLive
import zio._
import zio.console.putStrLn
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps

import scala.collection.immutable.HashSet

object Main extends App {

  val metaInfoFile      =
    "d:\\Torrents\\!torrent\\Breaking Bad - Season 1 [BDRip] (Кубик в Кубе).torrent"
  val dataDirectoryName = "c:\\!temp\\CopyTest\\"

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val effect = for {
      job <- Dispatcher.acquireJob(HashSet[PieceId](0, 1, 2))
      _   <- putStrLn(s"$job")
    } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      Slf4jLogger.make((_, message) => message),
      FixedBufferPool.make(32),
      FileIOLive.make(metaInfoFile, dataDirectoryName, allocateFiles = true),
      DispatcherLive.make
    )
      .catchAll(e => putStrLn(e.getMessage))
      .exitCode

  }
}
