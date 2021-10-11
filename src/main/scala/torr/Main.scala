package torr

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.slf4j.LoggerFactory
import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.announce.{Announce, AnnounceLive, Peer, TrackerRequest}
import torr.consoleui.SimpleConsoleUI
import torr.directbuffers.{DirectBufferPool, GrowableBufferPool}
import torr.dispatcher.{Dispatcher, DispatcherLive, PeerId}
import torr.examples.DownloadFromPeers.getClass
import torr.fileio.{FileIO, FileIOLive}
import torr.metainfo.MetaInfo
import torr.peerroutines.DefaultPeerRoutine
import torr.peerwire.PeerHandleLive
import zio.Exit.{Failure, Success}
import zio.{Cause, _}
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.putStrLn
import zio.duration.durationInt
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core._
import zio.random.Random

object Main extends App {

  val metaInfoFileName = "c:\\!temp\\ubuntu-21.04-desktop-amd64.iso.torrent"
  val dstDirectoryName = "c:\\!temp\\Ubuntu\\"

  val maxSimultaneousConnections = 100

  //noinspection SimplifyWhenInspection,SimplifyUnlessInspection
  def run(args: List[String]): URIO[zio.ZEnv with Blocking, ExitCode] = {

    configureLogging

    val effect =
      /*if (args.length != 1) putStrLn("Please specify a single torrent file name")
      else*/ download

    effect.injectCustom(
      ActorSystemLive.make("Default"),
      AnnounceLive.make(None),
      Slf4jLogger.make((_, message) => message),
      GrowableBufferPool.make(256),
      FileIOLive.make(metaInfoFileName, dstDirectoryName, allocateFiles = true),
      DispatcherLive.make,
      SimpleConsoleUI.make
    )
      .catchAll(e => putStrLn(e.getMessage))
      .exitCode
  }

  def download: ZIO[
    Dispatcher with FileIO with DirectBufferPool with Logging with Clock with ActorSystem with Announce with Random,
    Throwable,
    Unit
  ] = {
    for {
      myPeerId  <- makePeerId
      metaInfo  <- FileIO.metaInfo
      peerQueue <- Queue.unbounded[Peer]

      fetchPeersFiber <- fetchPeers(peerQueue, myPeerId, metaInfo).fork

      _ <- manageConnections(
             peerQueue,
             metaInfo,
             myPeerId,
             connectionFibers = Vector.empty[(Peer, Fiber[Throwable, Unit])]
           )

      _ <- fetchPeersFiber.interrupt
    } yield ()
  }

  def manageConnections(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      connectionFibers: Vector[(Peer, Fiber[Throwable, Unit])]
  ): ZIO[Dispatcher with FileIO with DirectBufferPool with Logging with Clock with ActorSystem, Throwable, Unit] = {
    Dispatcher.isDownloadCompleted.flatMap {
      case false => maintainActiveConnectionsCount(peerQueue, metaInfo, myPeerId, connectionFibers)
      case _     => ZIO.foreach_(connectionFibers) { case (_, fiber) => fiber.interrupt }
    }
  }

  def maintainActiveConnectionsCount(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      connectionFibers: Vector[(Peer, Fiber[Throwable, Unit])]
  ): ZIO[Dispatcher with FileIO with DirectBufferPool with Logging with Clock with ActorSystem, Throwable, Unit] = {
    if (connectionFibers.length < maxSimultaneousConnections) {
      peerQueue.poll.flatMap {
        case Some(peer) => establishConnection(peerQueue, peer, metaInfo, myPeerId, connectionFibers)
        case None       => processDropped(peerQueue, metaInfo, myPeerId, connectionFibers)
      }
    } else {
      processDropped(peerQueue, metaInfo, myPeerId, connectionFibers)
    }
  }

  def establishConnection(
      peerQueue: Queue[Peer],
      peer: Peer,
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      connections: Vector[(Peer, Fiber[Throwable, Unit])]
  ): ZIO[Dispatcher with FileIO with DirectBufferPool with Logging with Clock with ActorSystem, Throwable, Unit] = {
    val peerIdStr = peer.peerId
      .map(PeerHandleLive.makePeerIdStr)
      .fold("UNKNOWN")(identity)

    for {
      _       <- Logging.debug(s"$peerIdStr connecting to ${peer.ip}:${peer.port}")
      address <- InetSocketAddress.hostName(peer.ip, peer.port)
      fiber   <- PeerHandleLive.fromAddress(address, metaInfo.infoHash, myPeerId)
                   .use(DefaultPeerRoutine.run)
                   .fork
      _       <- manageConnections(peerQueue, metaInfo, myPeerId, connections.appended(peer, fiber))
    } yield ()
  }

  def processDropped(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      connectionFibers: Vector[(Peer, Fiber[Throwable, Unit])]
  ): ZIO[Dispatcher with FileIO with DirectBufferPool with Logging with Clock with ActorSystem, Throwable, Unit] = {

    for {
      updated <- ZIO.filter(connectionFibers) {
                   case (peer, fiber) =>
                     val peerIdStr = peer.peerId
                       .map(PeerHandleLive.makePeerIdStr)
                       .fold("UNKNOWN")(identity)

                     fiber.poll.flatMap {
                       case Some(Success(_))     =>
                         Logging.debug(s"$peerIdStr fiber successfully exited").as(false)
                       case Some(Failure(cause)) =>
                         Logging.debug(s"$peerIdStr fiber failed with $cause").as(false)
                       case None                 =>
                         ZIO.succeed(true)
                     }
                 }

      _       <- manageConnections(peerQueue, metaInfo, myPeerId, updated).delay(1.second)
    } yield ()
  }

  def fetchPeers(
      peerQueue: Queue[Peer],
      myPeerId: PeerId,
      metaInfo: MetaInfo
  ): RIO[Announce with Clock with Logging, Unit] = {

    val request = TrackerRequest(
      metaInfo.announce,
      metaInfo.infoHash,
      myPeerId,
      port = 12345,
      uploaded = 0,
      downloaded = 0,
      metaInfo.torrentSize
    )

    for {
      response <- Announce.update(request)
      _        <- Logging.debug(
                    s"ANNOUNCE got ${response.peers.length} peers. Interval = ${response.interval} seconds"
                  )
      _        <- peerQueue.offerAll(response.peers)
      _        <- ZIO.sleep(response.interval.seconds)
      _        <- fetchPeers(peerQueue, myPeerId, metaInfo)
    } yield ()
  }

  def makePeerId: RIO[Random, Chunk[Byte]] = {
    for {
      peerHash <- random.nextBytes(12)
    } yield Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
  }

  private def configureLogging: Unit = {
    val context      = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val configurator = new JoranConfigurator()
    configurator.setContext(context)
    context.reset()
    configurator.doConfigure(getClass.getResourceAsStream("/logback.xml"))
  }

}
