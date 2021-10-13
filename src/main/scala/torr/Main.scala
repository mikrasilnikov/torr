package torr

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.slf4j.LoggerFactory
import torr.Cli.{TorrArgs, TorrOptions}
import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.announce.{Announce, AnnounceLive, Peer, TrackerRequest}
import torr.consoleui.SimpleConsoleUI
import torr.directbuffers.{DirectBufferPool, GrowableBufferPool}
import torr.dispatcher.{Dispatcher, DispatcherLive, PeerId}
import torr.fileio.{FileIO, FileIOLive}
import torr.metainfo.MetaInfo
import torr.peerroutines.DefaultPeerRoutine
import torr.peerwire.PeerHandleLive
import zio.Exit.{Failure, Success}
import zio._
import zio.blocking.Blocking
import zio.cli.HelpDoc.Span._
import zio.cli._
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core._
import zio.random.Random

object Main extends App {

  type TorrEnv = Dispatcher
    with FileIO
    with DirectBufferPool
    with Logging
    with Clock
    with ActorSystem
    with Announce
    with Random

  //noinspection SimplifyWhenInspection,SimplifyUnlessInspection
  def run(args: List[String]): URIO[zio.ZEnv with Blocking, ExitCode] = {

    configureLogging

    val cliApp: CliApp[ZEnv, Throwable, (TorrOptions, TorrArgs)] =
      CliApp.make(
        name = "Torr",
        version = "0.0.1",
        summary = text("ZIO Bittorrent client"),
        command = Cli.default
      ) {
        case (options, args) =>
          val extension = ".torrent"

          val torrentFileAbsolutePath = args.metaInfoPath.toAbsolutePath.toString
          val dstFolderAbsolutePath   =
            if (torrentFileAbsolutePath.endsWith(extension))
              torrentFileAbsolutePath.take(torrentFileAbsolutePath.length - extension.length)
            else torrentFileAbsolutePath ++ ".out"

          download(options.maxConnections, options.maxActivePeers)
            .injectCustom(
              ActorSystemLive.make("Default"),
              AnnounceLive.make(options.proxy),
              Slf4jLogger.make((_, message) => message),
              GrowableBufferPool.make(256),
              FileIOLive.make(
                torrentFileAbsolutePath,
                dstFolderAbsolutePath
              ),
              DispatcherLive.make(options.maxActivePeers),
              SimpleConsoleUI.make
            ) //.catchAll(e => putStrLn(e.getMessage).orDie)
      }

    /*cliApp.run(
      "--help" :: Nil
    )*/

    cliApp.run(
      "-p" :: "127.0.0.1:8080" ::
        "c:\\!temp\\Афера века El robo del siglo The Great Heist.torrent" :: Nil
    )
  }

  def download(maxConnections: Int, maxActivePeers: Int): RIO[TorrEnv, Unit] = {
    for {
      myPeerId  <- makePeerId
      metaInfo  <- FileIO.metaInfo
      peerQueue <- Queue.unbounded[Peer]

      _ <- manageConnections(
             peerQueue,
             metaInfo,
             myPeerId,
             maxConnections,
             maxActivePeers,
             connections = Vector.empty[(Peer, Fiber[Throwable, Unit])]
           )
    } yield ()
  }

  def manageConnections(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      maxActivePeers: Int,
      connections: Vector[(Peer, Fiber[Throwable, Unit])]
  ): RIO[TorrEnv, Unit] = {
    Logging.debug("MAIN manageConnections") *>
      Dispatcher.isDownloadCompleted.flatMap {
        case false =>
          maintainActiveConnections(peerQueue, metaInfo, myPeerId, maxConnections, maxActivePeers, connections)
        case _     =>
          Logging.debug("MAIN interrupting all connections") *>
            ZIO.foreach_(connections) { case (_, fiber) => fiber.interrupt } *>
            Logging.debug("MAIN all connections interrupted")
      }
  }

  def maintainActiveConnections(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      maxActivePeers: Int,
      connections: Vector[(Peer, Fiber[Throwable, Unit])]
  ): RIO[TorrEnv, Unit] = {
    if (connections.length < maxConnections) {
      peerQueue.poll.flatMap {
        case Some(peer)                                  =>
          establishConnection(peerQueue, peer, metaInfo, myPeerId, maxConnections, maxActivePeers, connections)
        case None if connections.length < maxActivePeers =>
          fetchMorePeers(peerQueue, metaInfo, myPeerId, maxConnections, maxActivePeers, connections)
        case _                                           =>
          processDisconnected(peerQueue, metaInfo, myPeerId, maxConnections, maxActivePeers, connections)
      }
    } else {
      processDisconnected(peerQueue, metaInfo, myPeerId, maxConnections, maxActivePeers, connections)
    }
  }

  def establishConnection(
      peerQueue: Queue[Peer],
      peer: Peer,
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      maxActivePeers: Int,
      connections: Vector[(Peer, Fiber[Throwable, Unit])]
  ): RIO[TorrEnv, Unit] = {
    val peerIdStr = peer.peerId
      .map(PeerHandleLive.makePeerIdStr)
      .fold("UNKNOWN")(identity)

    for {
      _       <- Logging.debug(s"$peerIdStr connecting to ${peer.ip}:${peer.port}")
      address <- InetSocketAddress.hostName(peer.ip, peer.port)
      fiber   <- PeerHandleLive.fromAddress(address, metaInfo.infoHash, myPeerId)
                   .use(DefaultPeerRoutine.run)
                   .fork
      _       <- manageConnections(
                   peerQueue,
                   metaInfo,
                   myPeerId,
                   maxConnections,
                   maxActivePeers,
                   connections.appended(peer, fiber)
                 )
    } yield ()
  }

  def processDisconnected(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      maxActivePeers: Int,
      connections: Vector[(Peer, Fiber[Throwable, Unit])]
  ): RIO[TorrEnv, Unit] = {

    for {
      updated <- ZIO.filter(connections) {
                   case (peer, fiber) =>
                     val peerIdStr = peer.peerId
                       .map(PeerHandleLive.makePeerIdStr)
                       .fold("UNKNOWN")(identity)

                     fiber.poll.flatMap {
                       case Some(Success(_))     =>
                         Logging.debug(s"$peerIdStr fiber successfully exited").as(false)

                       case Some(Failure(cause)) =>
                         val messages = cause.failures
                           .map { throwable =>
                             val message = throwable.getMessage
                             if (message == null) ""
                             else message.strip
                           }
                           .mkString(",")

                         Logging.debug(s"$peerIdStr fiber failed: $messages").as(false)

                       case None                 =>
                         ZIO.succeed(true)
                     }
                 }

      _       <- manageConnections(
                   peerQueue,
                   metaInfo,
                   myPeerId,
                   maxConnections,
                   maxActivePeers,
                   connections = updated
                 ).delay(1.second)
    } yield ()
  }

  def fetchMorePeers(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      maxActivePeers: Int,
      connections: Vector[(Peer, Fiber[Throwable, Unit])]
  ): RIO[TorrEnv, Unit] = {

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
      _        <- manageConnections(peerQueue, metaInfo, myPeerId, maxConnections, maxActivePeers, connections)
                    .delay(10.seconds)
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
