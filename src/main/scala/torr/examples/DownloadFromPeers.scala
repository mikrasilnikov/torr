package torr.examples

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.slf4j.LoggerFactory
import torr.actorsystem.ActorSystemLive
import torr.consoleui.SimpleConsoleUI
import torr.directbuffers.GrowableBufferPool
import torr.dispatcher.DispatcherLive
import torr.fileio.{FileIO, FileIOLive}
import torr.peerroutines.PeerRoutine
import torr.peerwire.PeerHandleLive
import zio._
import zio.duration.durationInt
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.InetSocketAddress

object DownloadFromPeers extends App {

  val localPeers = List(
    ("localhost", 57617),
    //("localhost", 57618),
    ("localhost", 56233)
  )

  val remotePeers = List(
    ("178.162.139.101", 10084),
    ("185.21.216.160", 32774),
    ("82.64.150.155", 16881),
    ("51.175.174.178", 51413),
    ("185.21.217.47", 32805),
    ("176.169.227.239", 51413),
    ("78.92.49.195", 51413),
    ("79.49.125.246", 50000),
    ("185.21.217.47", 32808),
    ("182.235.166.107", 56655),
    ("46.63.85.161", 51413),
    ("72.21.17.22", 55664),
    ("84.53.254.170", 51413),
    ("185.107.95.84", 20006),
    ("185.21.216.187", 57283),
    ("185.148.3.185", 52710),
    ("71.254.196.102", 51413),
    ("104.254.90.235", 5563),
    ("97.87.134.7", 12357),
    ("198.27.64.129", 57659),
    ("188.209.56.10", 20247),
    ("148.251.2.71", 50000),
    ("46.98.0.78", 40110),
    ("2.39.5.250", 51135),
    ("89.230.79.188", 25005),
    ("78.27.108.101", 51413),
    ("78.70.140.174", 51413),
    ("164.58.9.151", 51413),
    ("98.142.45.10", 51413),
    ("85.135.116.106", 51413),
    ("159.224.183.37", 51413),
    ("91.179.230.170", 51413),
    ("185.107.71.5", 20053),
    ("91.121.87.148", 51413),
    ("142.161.137.43", 51413),
    ("5.196.75.7", 12383),
    ("72.21.17.7", 61285),
    ("185.203.56.42", 58409),
    ("185.21.217.54", 55230),
    ("108.72.45.195", 51413),
    ("222.131.58.120", 50609),
    ("185.144.29.198", 39909),
    ("147.83.140.45", 51413),
    ("91.121.7.132", 51413),
    ("67.183.10.134", 30020),
    ("50.53.234.20", 51413),
    ("89.23.141.201", 44104),
    ("78.29.35.166", 53931),
    ("72.21.17.9", 64137),
    ("136.35.211.9", 61434)
  )

  //val metaInfoFile     = "d:\\Torrents\\!torrent\\Good.Will.Hunting.1997.BDRip.avi.torrent"
  //val dstDirectoryName = "c:\\!temp\\CopyTest1\\"

  //val metaInfoFile     = "d:\\Torrents\\!torrent\\Breaking Bad - Season 1 [BDRip] (Кубик в Кубе).torrent"
  //val dstDirectoryName = "c:\\!temp\\CopyTest2\\"

  val metaInfoFile     = "C:\\!temp\\ubuntu-21.04-desktop-amd64.iso.torrent"
  val dstDirectoryName = "c:\\!temp\\Ubuntu\\"

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    configureLogging

    val effect = for {
      peerHash   <- random.nextBytes(12)
      localPeerId = Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
      metaInfo   <- FileIO.metaInfo
      _          <- ZIO.foreachPar_(remotePeers) {
                      case (host, port) =>
                        for {
                          address <- InetSocketAddress.hostName(host, port)
                          _       <- PeerHandleLive.fromAddress(address, metaInfo.infoHash, localPeerId)
                                       .use {
                                         handle => PeerRoutine.run(handle)
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
      DispatcherLive.make(10, 5),
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
