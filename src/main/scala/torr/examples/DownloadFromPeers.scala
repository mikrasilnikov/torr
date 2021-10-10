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
    ("195.46.165.41", 51413),
    ("185.21.217.11", 32785),
    ("77.13.17.16", 54412),
    ("129.146.73.52", 51413),
    ("89.45.46.39", 20991),
    ("142.44.143.59", 51413),
    ("91.179.230.170", 51413),
    ("173.49.241.161", 51413),
    ("109.71.204.227", 6881),
    ("198.54.131.68", 54911),
    ("154.3.40.34", 30065),
    ("81.39.111.105", 51413),
    ("5.196.75.7", 12383),
    ("176.9.101.204", 23496),
    ("2.39.5.250", 51135),
    ("86.200.165.236", 51413),
    ("72.21.17.60", 55319),
    ("173.160.196.221", 6890),
    ("37.17.168.78", 58308),
    ("185.102.188.7", 51111),
    ("199.48.94.240", 51413),
    ("212.102.39.72", 28412),
    ("84.255.243.215", 33333),
    ("94.231.0.134", 49300),
    ("24.171.109.44", 64959),
    ("45.124.201.25", 51419),
    ("185.162.184.34", 51267),
    ("136.34.171.176", 51413),
    ("79.213.205.207", 52525),
    ("194.166.96.47", 51413),
    ("185.132.74.130", 65047),
    ("62.210.113.157", 12621),
    ("72.21.17.22", 62858),
    ("86.144.172.125", 51413),
    ("143.244.41.192", 23851),
    ("185.144.29.198", 39909),
    ("72.21.17.9", 64137),
    ("62.78.187.78", 9019),
    ("88.212.170.12", 51413),
    ("212.47.227.30", 51413),
    ("129.146.122.241", 51413),
    ("85.7.160.57", 51316),
    ("185.156.175.171", 17977),
    ("82.65.87.157", 51413),
    ("185.21.217.17", 53690),
    ("90.248.171.222", 51413),
    ("37.187.109.201", 60185),
    ("83.49.142.192", 4666),
    ("98.128.200.156", 12020),
    ("95.168.168.160", 48209)
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
