package torr.examples

import torr.actorsystem.ActorSystemLive
import torr.announce._
import torr.directbuffers.GrowableBufferPool
import torr.dispatcher.DispatcherLive
import torr.fileio.{FileIO, FileIOLive}
import zio._
import zio.console.putStrLn
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps

object AskTrackerForPeers extends App {

  val metaInfoFile     = "C:\\!temp\\ubuntu-21.04-desktop-amd64.iso.torrent"
  val dstDirectoryName = "c:\\!temp\\Ubuntu\\"

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val effect =
      for {
        peerHash   <- random.nextBytes(12)
        localPeerId = Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
        metaInfo   <- FileIO.metaInfo
        request     = TrackerRequest(metaInfo.announce, metaInfo.infoHash, localPeerId, 12345, 0, 0, metaInfo.torrentSize)
        response   <- Announce.update(request)
        result      = response.peers.map(p => s"(${p.ip}, ${p.port})").mkString(",\n")
        _          <- putStrLn(s"$result")
      } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      AnnounceLive.make(None),
      Slf4jLogger.make((_, message) => message),
      GrowableBufferPool.make(512),
      FileIOLive.make(metaInfoFile, dstDirectoryName)
    ).exitCode
  }

}
