package torr.examples

import torr.actorsystem.ActorSystemLive
import torr.directbuffers.{DirectBufferPool, FixedBufferPool, GrowableBufferPool}
import torr.dispatcher.DispatcherLive
import torr.fileio.{FileIO, FileIOLive}
import torr.metainfo.MetaInfo
import torr.peerroutines.{DefaultPeerRoutine, PipelineDownloadRoutine}
import torr.peerwire.{Message, PeerHandle, PeerHandleLive}
import torr.peerwire.MessageTypes._
import zio.Cause.Internal
import zio._
import zio.console.{Console, putStrLn}
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.{InetAddress, InetSocketAddress}

import scala.collection.immutable.HashMap

object DownloadFromPeers extends App {

  val localPeers = List(
    ("localhost", 57617),
    ("localhost", 57618)
    //("localhost", 57619)
  )

  val remotePeers = List(
    ("188.165.197.41", 51413),
    ("217.64.148.122", 51409),
    ("212.102.39.72", 28412),
    ("77.22.147.130", 51425),
    ("86.87.229.102", 51413),
    ("185.207.166.93", 62427),
    ("155.138.254.162", 51413),
    ("188.126.94.86", 40892),
    ("91.121.159.144", 45003),
    ("5.9.40.146", 33869),
    ("182.235.166.107", 60516)
  )

  /*val metaInfoFile     =
    "d:\\Torrents\\!torrent\\Breaking Bad - Season 1 [BDRip] (Кубик в Кубе).torrent"
  val dstDirectoryName = "c:\\!temp\\CopyTest1\\"*/

  val metaInfoFile     = "C:\\!temp\\ubuntu-21.04-desktop-amd64.iso.torrent"
  val dstDirectoryName = "c:\\!temp\\Ubuntu\\"

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val effect = for {
      peerHash   <- random.nextBytes(12)
      localPeerId = Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
      metaInfo   <- FileIO.metaInfo
      _          <- ZIO.foreachPar_(remotePeers) {
                      case (host, port) =>
                        for {
                          address <- InetSocketAddress.hostName(host, port)
                          _       <- PeerHandleLive.fromAddress(address, metaInfo.infoHash, localPeerId)
                                       .use(handle => DefaultPeerRoutine.run(handle))
                                       .onError(e => ZIO.foreach_(e.failures)(t => putStrLn(t.getMessage).orDie))
                                       .ignore
                        } yield ()
                    }
      _          <- FileIO.flush
    } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      //Slf4jLogger.make((_, message) => message),
      GrowableBufferPool.make(512),
      FileIOLive.make(metaInfoFile, dstDirectoryName),
      DispatcherLive.make
    ).exitCode
  }

}
