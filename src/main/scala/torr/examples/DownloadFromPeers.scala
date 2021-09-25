package torr.examples

import torr.actorsystem.ActorSystemLive
import torr.directbuffers.{DirectBufferPool, DirectBufferPoolLive}
import torr.dispatcher.DispatcherLive
import torr.fileio.{FileIO, FileIOLive}
import torr.metainfo.MetaInfo
import torr.peerproc.DefaultPeerProc
import torr.peerwire.{Message, PeerHandle}
import torr.peerwire.MessageTypes._
import zio._
import zio.console.{Console, putStrLn}
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.{InetAddress, InetSocketAddress}

import scala.collection.immutable.HashMap

object DownloadFromPeers extends App {

  val peers = List(
    ("localhost", 57617)
  )

  val metaInfoFile     =
    "d:\\Torrents\\!torrent\\Breaking Bad - Season 1 [BDRip] (Кубик в Кубе).torrent"
  val dstDirectoryName = "c:\\!temp\\CopyTest1\\"

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val effect = for {
      peerHash   <- random.nextBytes(12)
      localPeerId = Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
      metaInfo   <- FileIO.metaInfo
      _          <- ZIO.foreachPar_(peers) {
                      case (host, port) =>
                        for {
                          address <- InetSocketAddress.hostName(host, port)
                          _       <- PeerHandle.fromAddress(address, metaInfo.infoHash, localPeerId)
                                       .use(handle => DefaultPeerProc.run(handle))
                        } yield ()
                    }
      _          <- FileIO.flush
    } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      Slf4jLogger.make((_, message) => message),
      DirectBufferPoolLive.make(32),
      FileIOLive.make(metaInfoFile, dstDirectoryName),
      DispatcherLive.make
    ).exitCode
  }

}
