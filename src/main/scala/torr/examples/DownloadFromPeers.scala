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
    ("83.227.1.92", 16881),
    ("123.145.67.154", 51413),
    ("185.21.217.22", 63372),
    ("185.148.3.139", 59055),
    ("88.127.148.82", 51413),
    ("98.162.154.133", 11599),
    ("163.172.219.226", 59974),
    ("90.248.171.222", 51413),
    ("37.48.95.151", 45288),
    ("51.15.176.135", 51413),
    ("104.254.90.235", 5563),
    ("213.163.240.19", 51413),
    ("88.88.21.196", 3690),
    ("91.121.87.148", 51413),
    ("93.226.92.238", 51413),
    ("198.27.67.207", 51115),
    ("77.56.26.88", 45353),
    ("176.9.101.204", 23496),
    ("85.21.233.197", 50000),
    ("84.126.222.41", 51413),
    ("188.165.212.60", 51413),
    ("209.141.42.18", 51413),
    ("5.196.75.7", 12383),
    ("37.113.182.54", 51413),
    ("192.145.124.21", 9980),
    ("76.177.233.118", 49161),
    ("182.214.75.172", 16881),
    ("188.209.56.10", 20247),
    ("142.184.65.134", 51413),
    ("188.165.197.41", 51413),
    ("148.251.181.206", 6882),
    ("116.82.102.140", 51413),
    ("118.189.203.50", 46821),
    ("108.6.120.138", 63622),
    ("77.220.109.123", 51413),
    ("85.19.185.250", 40001),
    ("80.108.4.231", 16881),
    ("172.98.71.22", 39876),
    ("185.107.71.5", 20053),
    ("80.78.21.245", 51413),
    ("148.251.2.71", 50000),
    ("93.42.249.143", 51413),
    ("118.158.54.30", 56892),
    ("188.209.56.7", 20103),
    ("185.21.217.55", 59487),
    ("98.142.45.10", 51413),
    ("37.17.168.78", 58308),
    ("129.146.73.52", 51413),
    ("72.21.17.9", 64137),
    ("87.189.109.1", 51410)
  )

  //val metaInfoFile     =
  //  "d:\\Torrents\\!torrent\\Good.Will.Hunting.1997.BDRip.avi.torrent"
  //val dstDirectoryName = "c:\\!temp\\CopyTest1\\"

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
                                       .use(handle => DefaultPeerRoutine.run(handle))
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
