package torr.examples

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.slf4j.LoggerFactory
import torr.actorsystem.ActorSystemLive
import torr.consoleui.SimpleConsoleUI
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
import zio.duration.durationInt
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.{InetAddress, InetSocketAddress}

import scala.collection.immutable.HashMap

object DownloadFromPeers extends App {

  val localPeers = List(
    ("localhost", 57617),
    //("localhost", 57618),
    ("localhost", 56233)
  )

  val remotePeers = List(
    ("37.59.60.221", 6917),
    ("185.21.216.196", 26280),
    ("58.158.0.86", 62799),
    ("78.31.187.202", 51413),
    ("45.124.144.222", 51231),
    ("81.201.59.85", 51386),
    ("46.232.210.74", 62542),
    ("2.204.62.95", 51413),
    ("77.123.42.148", 51413),
    ("78.22.124.223", 51413),
    ("134.249.153.220", 49164),
    ("90.248.123.180", 51413),
    ("78.83.44.72", 65001),
    ("188.209.56.46", 20184),
    ("146.115.161.94", 51413),
    ("79.246.246.190", 51413),
    ("172.94.12.194", 51413),
    ("86.144.172.125", 51413),
    ("91.189.95.21", 6931),
    ("188.209.56.50", 20045),
    ("195.154.164.249", 51230),
    ("24.17.29.208", 51413),
    ("84.84.96.92", 51515),
    ("82.197.213.132", 32767),
    ("37.48.95.62", 57383)
  )

  //val metaInfoFile     =
  //   "d:\\Torrents\\!torrent\\Good.Will.Hunting.1997.BDRip.avi.torrent"
  //val dstDirectoryName = "c:\\!temp\\CopyTest1\\"

  val metaInfoFile     =
    "d:\\Torrents\\!torrent\\Breaking Bad - Season 1 [BDRip] (Кубик в Кубе).torrent"
  val dstDirectoryName = "c:\\!temp\\CopyTest2\\"

  //val metaInfoFile     = "C:\\!temp\\ubuntu-21.04-desktop-amd64.iso.torrent"
  //val dstDirectoryName = "c:\\!temp\\Ubuntu\\"

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    configureLogging

    val effect = for {
      peerHash   <- random.nextBytes(12)
      localPeerId = Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
      metaInfo   <- FileIO.metaInfo
      _          <- ZIO.foreachPar_(localPeers) {
                      case (host, port) =>
                        for {
                          address <- InetSocketAddress.hostName(host, port)
                          _       <- PeerHandleLive.fromAddress(address, metaInfo.infoHash, localPeerId)
                                       .use {
                                         handle => DefaultPeerRoutine.run(handle)
                                       }

                                       //.onError(e => ZIO.foreach_(e.failures)(t => putStrLn(t.getMessage).orDie))
                                       .ignore
                        } yield ()
                    }
      _          <- FileIO.flush.delay(1.second)
    } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      Slf4jLogger.make((_, message) => message),
      GrowableBufferPool.make(512),
      FileIOLive.make(metaInfoFile, dstDirectoryName),
      DispatcherLive.make,
      SimpleConsoleUI.make
    ).exitCode
  }

  private def configureLogging: Unit = {
    val context      = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val configurator = new JoranConfigurator()
    configurator.setContext(context)
    context.reset()
    configurator.doConfigure(getClass.getResourceAsStream("/logback.xml"))
  }
}
