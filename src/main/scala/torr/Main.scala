package torr

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.slf4j.LoggerFactory
import torr.Cli.{TorrArgs, TorrOptions}
import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.announce.{Announce, AnnounceLive, Peer, TrackerRequest}
import torr.consoleui.SimpleConsoleUI
import torr.directbuffers.{DirectBufferPool, GrowableBufferPool}
import torr.dispatcher.{Dispatcher, DispatcherLive, DownloadCompletion, PeerId}
import torr.fileio.{FileIO, FileIOLive}
import torr.metainfo.MetaInfo
import torr.peerroutines.DefaultPeerRoutine
import torr.peerwire.PeerHandleLive
import zio.Exit.{Failure, Success}
import zio.{Cause, _}
import zio.blocking.Blocking
import zio.cli.HelpDoc.Span._
import zio.cli._
import zio.clock.Clock
import zio.duration.durationInt
import zio.logging.Logging
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core._
import zio.nio.core.file.Path
import zio.nio.file.Files
import zio.random.Random

import java.io.IOException
import java.nio.charset.StandardCharsets
import java.nio.file.{OpenOption, StandardOpenOption}
import scala.collection.immutable.HashMap

object Main extends App {

  type TorrEnv = Dispatcher
    with FileIO
    with DirectBufferPool
    with Logging
    with Clock
    with ActorSystem
    with Announce
    with Random
    with Blocking

  case class PeerConnection(peerId: Promise[Throwable, PeerId], fiber: Fiber[Throwable, Unit])

  //noinspection SimplifyWhenInspection,SimplifyUnlessInspection
  def run(args: List[String]): URIO[zio.ZEnv with Blocking, ExitCode] = {

    configureLogging()

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

    cliApp.run(args)

    /*cliApp.run(
      "--px" :: "127.0.0.1:8080" ::
        "d:\\!temp\\Good.Will.Hunting.1997.BDRip.avi.torrent" :: Nil
    )*/
  }

  def download(maxConnections: Int, maxActivePeers: Int): RIO[TorrEnv, Unit] = {
    for {
      myPeerId  <- makePeerId
      metaInfo  <- FileIO.metaInfo
      peerQueue <- Queue.unbounded[Peer]

      announceFib <- fetchMorePeers(peerQueue, metaInfo, myPeerId).fork

      _ <- manageConnections(
             peerQueue,
             metaInfo,
             myPeerId,
             maxConnections,
             connections = HashMap.empty[Peer, PeerConnection]
           )

      _ <- announceFib.interrupt

    } yield ()
  }

  def manageConnections(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      connections: HashMap[Peer, PeerConnection]
  ): RIO[TorrEnv, Unit] = {
    Dispatcher.isDownloadCompleted.flatMap {
      case DownloadCompletion.Completed =>
        for {
          _ <- ZIO.sleep(3.seconds)
          _ <- Logging.debug("MAIN interrupting all connections")
          _ <- ZIO.foreach_(connections) { case (_, PeerConnection(_, fiber)) => fiber.interrupt }
          _ <- Logging.debug("MAIN all connections interrupted")
        } yield ()

      case _                            =>
        maintainActiveConnections(
          peerQueue,
          metaInfo,
          myPeerId,
          maxConnections,
          connections
        )
    }
  }

  def maintainActiveConnections(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      connections: HashMap[Peer, PeerConnection]
  ): RIO[TorrEnv, Unit] = {
    if (connections.size < maxConnections) {
      peerQueue.poll.flatMap {
        // Tracker may return a peer that we are already connected to.
        case Some(peer) if !connections.contains(peer) =>
          establishConnection(peerQueue, peer, metaInfo, myPeerId, maxConnections, connections)

        case Some(_)                                   =>
          maintainActiveConnections(peerQueue, metaInfo, myPeerId, maxConnections, connections)

        case _                                         =>
          handleDisconnected(peerQueue, metaInfo, myPeerId, maxConnections, connections)
      }
    } else {
      handleDisconnected(peerQueue, metaInfo, myPeerId, maxConnections, connections)
    }
  }

  def establishConnection(
      peerQueue: Queue[Peer],
      peer: Peer,
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      connections: HashMap[Peer, PeerConnection]
  ): RIO[TorrEnv, Unit] = {
    val peerIdStr = peer.peerId
      .map(PeerHandleLive.makePeerIdStr)
      .fold("UNKNOWN")(identity)

    for {
      _       <- Logging.debug(s"$peerIdStr connecting to ${peer.ip}:${peer.port}")
      address <- InetSocketAddress.hostName(peer.ip, peer.port)

      // Some trackers do not return peerIds in their responses.
      // So the only reliable way to find out remote peerId is to ask remote peer for it.
      // If connection would be established, peerIdP would be completed with received peer id.
      peerIdP <- Promise.make[Throwable, PeerId]
      fiber   <- PeerHandleLive.fromAddress(address, metaInfo.infoHash, myPeerId)
                   .use { peerHandle =>
                     peerIdP.succeed(peerHandle.peerId) *>
                       DefaultPeerRoutine.run(peerHandle)
                   }
                   .fork

      _       <- manageConnections(
                   peerQueue,
                   metaInfo,
                   myPeerId,
                   maxConnections,
                   connections + (peer -> PeerConnection(peerIdP, fiber))
                 )
    } yield ()
  }

  def handleDisconnected(
      peerQueue: Queue[Peer],
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      maxConnections: Int,
      connections: HashMap[Peer, PeerConnection]
  ): RIO[TorrEnv, Unit] = {

    for {
      // This .toList is necessary because ZIO.filter does not work with HashMap.
      // It wont affect performance much because of relatively small amount of active connections (~100).
      updated <- ZIO.filter(connections.toList) {
                   case (_, PeerConnection(peerIdP, fiber)) =>
                     for {
                       peerIdOption <- peerIdP.isDone.flatMap {
                                         case true => peerIdP.await.map(pid => Some(PeerHandleLive.makePeerIdStr(pid)))
                                         case _    => ZIO.none
                                       }

                       res          <- fiber.poll.flatMap {
                                         case Some(Success(_)) if peerIdOption.isDefined     =>
                                           Logging.debug(s"${peerIdOption.get} fiber successfully exited")
                                             .as(false)

                                         case Some(Success(_))                               =>
                                           Logging.debug(s"UNKNOWN fiber successfully exited")
                                             .as(false)

                                         // Saving errors only for peers that we have been connected to.
                                         case Some(Failure(cause)) if peerIdOption.isDefined =>
                                           val messages = messagesFromCause(cause)
                                           for {
                                             uuid <- saveError(peerIdOption.get, cause)
                                             _    <- Logging.debug(
                                                       s"${peerIdOption.get} fiber failed: $messages. Details in $uuid.log"
                                                     )
                                           } yield false

                                         case Some(Failure(_))                               =>
                                           ZIO.succeed(false)

                                         case None                                           =>
                                           ZIO.succeed(true)
                                       }
                     } yield res
                 }

      _       <- manageConnections(
                   peerQueue,
                   metaInfo,
                   myPeerId,
                   maxConnections,
                   connections = HashMap.from(updated)
                 ).delay(1.second)
    } yield ()
  }

  def saveError(peerIdStr: String, cause: Cause[Throwable]): RIO[Blocking, String] = {
    val errorsPath = Path("errors")
    val uuid       = java.util.UUID.randomUUID.toString
    val filePath   = errorsPath / Path(s"$uuid.log")

    for {
      _      <- Files.createDirectory(errorsPath).whenM(Files.notExists(errorsPath))
      dataStr = s"$peerIdStr failed with Cause:\n" ++ cause.prettyPrint
      data    = Chunk.fromArray(dataStr.getBytes(StandardCharsets.UTF_8))
      _      <- Files.writeBytes(filePath, data, StandardOpenOption.CREATE)
    } yield uuid
  }

  def fetchMorePeers(peerQueue: Queue[Peer], metaInfo: MetaInfo, myPeerId: PeerId): RIO[TorrEnv, Unit] = {

    val request = TrackerRequest(
      metaInfo.announce,
      metaInfo.infoHash,
      myPeerId,
      port = 12345,
      uploaded = 0,
      downloaded = 0,
      metaInfo.torrentSize
    )

    peerQueue.size.flatMap {
      case s if s > 0 =>
        Logging.debug(s"ANNOUNCE peerQueue size is $s") *>
          fetchMorePeers(peerQueue, metaInfo, myPeerId).delay(1.minute)
      case _          =>
        Announce.update(request).fork
          .flatMap(fiber => fiber.await)
          .flatMap {
            case Success(response) =>
              for {
                _ <- Logging.debug(s"ANNOUNCE got ${response.peers.length} peers")
                _ <- peerQueue.offerAll(response.peers)
                _ <- fetchMorePeers(peerQueue, metaInfo, myPeerId).delay(1.minute)
              } yield ()

            case Failure(cause)    =>
              val messages = cause.failures
                .map { throwable =>
                  val message = throwable.getMessage
                  if (message == null) "" else message.strip
                }.mkString(",")

              for {
                uuid <- saveError("ANNOUNCE", cause)
                _    <- Logging.debug(s"ANNOUNCE failed with $messages, log saved to $uuid.log")
                _    <- fetchMorePeers(peerQueue, metaInfo, myPeerId).delay(1.minute)
              } yield ()
          }
    }
  }

  def makePeerId: RIO[Random, Chunk[Byte]] = {
    for {
      peerHash <- random.nextBytes(12)
    } yield Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
  }

  private def configureLogging(): Unit = {
    val context      = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val configurator = new JoranConfigurator()
    configurator.setContext(context)
    context.reset()
    configurator.doConfigure(getClass.getResourceAsStream("/logback.xml"))
  }

  private def messagesFromCause(cause: Cause[Throwable]): String = {
    cause.failures
      .map { throwable =>
        val message = throwable.getMessage
        if (message == null) ""
        else message.strip
      }.mkString(",")
  }
}
