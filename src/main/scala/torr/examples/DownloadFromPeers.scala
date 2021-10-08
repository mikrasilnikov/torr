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
  )

  val metaInfoFile     =
    "d:\\Torrents\\!torrent\\Good.Will.Hunting.1997.BDRip.avi.torrent"
  val dstDirectoryName = "c:\\!temp\\CopyTest1\\"

  /*val metaInfoFile     = "C:\\!temp\\ubuntu-21.04-desktop-amd64.iso.torrent"
  val dstDirectoryName = "c:\\!temp\\Ubuntu\\"*/

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
