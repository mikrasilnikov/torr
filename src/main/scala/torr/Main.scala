package torr

import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.classic.joran.JoranConfigurator
import org.slf4j.LoggerFactory
import torr.Cli.{TorrArgs, TorrOptions}
import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.announce.{Announce, AnnounceLive, Peer, TrackerRequest}
import torr.channels.{AsyncSocketChannel, ByteChannel}
import torr.consoleui.{ConsoleUI, RichConsoleUI, SimpleConsoleUI}
import torr.directbuffers.{DirectBufferPool, GrowableBufferPool}
import torr.dispatcher.{Dispatcher, DispatcherLive, DownloadCompletion, PeerId}
import torr.fileio.{FileIO, FileIOLive}
import torr.metainfo.MetaInfo
import torr.peerroutines.PeerRoutine
import torr.peerwire.PeerHandleLive
import zio.Exit.{Failure, Success}
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
import zio._
import zio.console.{Console, putStrLn}
import zio.nio.core.channels.{AsynchronousServerSocketChannel, AsynchronousSocketChannel}

import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption
import java.util.concurrent.ConcurrentHashMap
import scala.collection.convert.ImplicitConversions.{`enumeration AsScalaIterator`, `map AsScalaConcurrentMap`}

object Main extends App {

  type TorrEnv = Dispatcher
    with FileIO
    with DirectBufferPool
    with Logging
    with Clock
    with ActorSystem
    with ConsoleUI
    with Announce
    with Random
    with Blocking

  case class PeerConnection(peerIdPromise: Promise[Throwable, PeerId], fiber: Fiber[Throwable, Unit]) {
    def peerIdStr: Task[String] =
      peerIdPromise.poll.flatMap {
        case Some(io) => io.map(PeerHandleLive.makePeerIdStr)
        case None     => ZIO.succeed("UNKNOWN")
      }
  }

  //noinspection SimplifyWhenInspection,SimplifyUnlessInspection
  def run(args: List[String]): URIO[ZEnv, ExitCode] = {

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

          main(options, args)
            .injectCustom(
              ActorSystemLive.make("Default"),
              AnnounceLive.make(options.proxy),
              Slf4jLogger.make((_, message) => message),
              GrowableBufferPool.make(256),
              FileIOLive.make(
                torrentFileAbsolutePath,
                dstFolderAbsolutePath
              ),
              DispatcherLive.make(
                options.maxSimultaneousDownloads,
                options.maxSimultaneousUploads
              ),
              //SimpleConsoleUI.make
              RichConsoleUI.make(options.maxConnections)
            )
      }

    args match {
      case "--help" :: Nil =>
        (
          putStrLn("Usage: ") *>
            putStrLn(
              "java -jar torr.jar [--port|-p listenPort] [--maxConn|-c maxConnections] [--proxy proxyAddr] " +
                "[--maxDown maxSimultaneousDownloads] [--maxUp maxSimultaneousUploads] torrentFile additionalPeer ...\n"
            ) *>
            putStrLn("Example:") *>
            putStrLn("" +
              "java -jar torr.jar --port 55123 --maxConn 500 --proxy 127.0.0.1:8080 --maxDown 20 --maxUp 20 " +
              "ubuntu-21.04-desktop-amd64.iso.torrent 217.111.45.01:54184 217.111.45.02:41265")
        ).exitCode

      case args            => cliApp.run(args)
      /*cliApp.run(
          //"--proxy" :: "127.0.0.1:8080" ::
          "c:\\!temp\\kubuntu-21.04-desktop-amd64.iso.torrent" ::
            // "127.0.0.1:56233" :: "127.0.0.1:57617" ::
            Nil
        )*/
    }
  }

  def main(options: TorrOptions, args: TorrArgs): RIO[TorrEnv, Unit] = {
    for {
      myPeerId <- makePeerId
      metaInfo <- FileIO.metaInfo

      connections = new ConcurrentHashMap[Peer, PeerConnection]()

      _ <- ZIO.foreachPar_(args.additionalPeers)(peer => establishConnection(peer, metaInfo, myPeerId, connections))

      _ <- ZIO.never // For pretty scalafmt indentation.
             // Neither of these effects should ever complete.
             // If any of these does exit we consider it a failure.
             .raceFirst(connectToPeers(metaInfo, myPeerId, options.port, options.maxSimultaneousDownloads, connections))
             .raceFirst(acceptConnections(metaInfo, myPeerId, options.port, options.maxConnections, connections))
             .raceFirst(handleDisconnected(connections))
             .raceFirst(drawConsoleUI)
             .foldCauseM(
               cause => saveError(cause, "MAIN", "main"),
               _ => Logging.error("MAIN exited unexpectedly")
             )

    } yield ()
  }

  private def connectToPeers(
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      myPort: Int,
      maxUploadingPeers: Int,
      connections: ConcurrentHashMap[Peer, PeerConnection]
  ): RIO[TorrEnv, Unit] = {

    def request =
      TrackerRequest(
        metaInfo.announce,
        metaInfo.infoHash,
        myPeerId,
        port = myPort,
        // It does not report stats to tracker.
        uploaded = 0,
        downloaded = 0,
        left = metaInfo.torrentSize
      )

    (Dispatcher.isDownloadCompleted <*> Dispatcher.numUploadingPeers).flatMap {

      // We don't need to search for more peers if download is completed.
      // At the same time, this fiber should not exit because exit signals failure by convention.
      case (DownloadCompletion.Completed, _) => ZIO.never

      // All download slots are occupied.
      case (_, n) if n >= maxUploadingPeers =>
        for {
          _ <- Logging.debug(s"ANNOUNCE numUploadingPeers = $n, tracker querying is not necessary")
          _ <- connectToPeers(metaInfo, myPeerId, myPort, maxUploadingPeers, connections).delay(10.seconds)
        } yield ()

      case _                                =>
        Announce.update(request)
          .foldCauseM(
            cause =>
              saveError(cause, "ANNOUNCE", "announce") *>
                connectToPeers(metaInfo, myPeerId, myPort, maxUploadingPeers, connections).delay(1.minute),
            response =>
              for {
                _ <- Logging.debug(s"ANNOUNCE got ${response.peers.length} peers")
                _ <- ZIO.foreachPar_(response.peers)(peer =>
                       establishConnection(peer, metaInfo, myPeerId, connections)
                     ).orDie // establishConnection(...) must not fail.
                _ <- connectToPeers(metaInfo, myPeerId, myPort, maxUploadingPeers, connections).delay(1.minute)
              } yield ()
          )
    }
  }

  private def establishConnection(
      peer: Peer,
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      connections: ConcurrentHashMap[Peer, PeerConnection]
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
                       PeerRoutine.run(peerHandle)
                   }
                   .fork

      _        = connections.put(peer, PeerConnection(peerIdP, fiber))
    } yield ()
  }

  private def acceptConnections(
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      myPort: Int,
      maxActiveConnections: Int,
      connections: ConcurrentHashMap[Peer, PeerConnection]
  ): RIO[TorrEnv, Unit] = {

    // Modified example from
    // https://zio.github.io/zio-nio/docs/essentials/essentials_sockets
    AsynchronousServerSocketChannel.open
      .mapM { socket =>
        for {
          address <- InetSocketAddress.wildCard(myPort)
          _       <- socket.bindTo(address)

          _ <- socket.accept.preallocate.flatMap(_.use(channel =>
                 acceptConnection(channel, metaInfo, myPeerId, connections)
                   .foldCauseM(
                     cause => saveError(cause, "UNKNOWN", "accept"),
                     _ => ZIO.unit
                   )
                   .when(connections.size() < maxActiveConnections)
               ).fork).forever.fork
        } yield ()
      }.useForever
  }

  private def acceptConnection(
      socketChannel: AsynchronousSocketChannel,
      metaInfo: MetaInfo,
      myPeerId: PeerId,
      connections: ConcurrentHashMap[Peer, PeerConnection]
  ): RIO[TorrEnv, Unit] = {

    socketChannel.remoteAddress.flatMap {
      case None          => ZIO.unit
      case Some(address) =>
        val socketAddress = address.asInstanceOf[InetSocketAddress]
        for {
          host       <- socketAddress.hostName
          port        = socketAddress.port
          peer        = Peer(host, port)
          channelName = s"$host:$port"
          _          <- Logging.debug(s"UNKNOWN accepting connection from $channelName")
          byteChannel = AsyncSocketChannel(socketChannel)
          peerIdP    <- Promise.make[Throwable, PeerId]
          fiber      <- PeerHandleLive.fromChannelWithHandshake(
                          byteChannel,
                          socketAddress,
                          channelName,
                          metaInfo.infoHash,
                          myPeerId
                        )
                          .use { peerHandle =>
                            peerIdP.succeed(peerHandle.peerId) *>
                              PeerRoutine.run(peerHandle)
                          }
                          .fork

          _           = connections.put(peer, PeerConnection(peerIdP, fiber))
          _          <- fiber.await
        } yield ()
    }
  }

  private def drawConsoleUI: RIO[TorrEnv, Unit] = {
    for {
      _ <- ConsoleUI.clear
      _ <- ConsoleUI.draw
      _ <- drawConsoleUI.delay(1.second)
    } yield ()
  }

  private def handleDisconnected(
      connections: ConcurrentHashMap[Peer, PeerConnection]
  ): RIO[TorrEnv, Unit] = {

    def loop(keys: List[Peer]): RIO[Logging with Blocking, Unit] = {
      keys match {
        case Nil          => ZIO.unit
        case peer :: tail =>
          val connection = connections.get(peer)
          for {
            peerId <- connection.peerIdStr

            _ <- connection.fiber.poll.flatMap {
                   case None => loop(tail)

                   case Some(Success(_))     =>
                     connections.remove(peer)
                     Logging.debug(s"$peerId fiber successfully exited") *> loop(tail)

                   case Some(Failure(cause)) =>
                     connections.remove(peer)
                     saveError(cause, peerId, "peer") *> loop(tail)
                 }
          } yield ()
      }
    }

    for {
      _ <- loop(connections.keys().toList)
      _ <- handleDisconnected(connections).delay(1.second)
    } yield ()
  }

  private def saveError(
      cause: Cause[Throwable],
      logMessagePrefix: String,
      traceFilePrefix: String
  ): RIO[Logging with Blocking, String] = {

    val messages = messagesFromCause(cause)

    val errorsPath = Path("errors")
    val uuid       = java.util.UUID.randomUUID.toString
    val filePath   = errorsPath / Path(s"$traceFilePrefix-$uuid.log")

    for {
      _      <- Logging.error(s"$logMessagePrefix failed with $messages, log saved to $traceFilePrefix-$uuid.log")
      _      <- Files.createDirectory(errorsPath).whenM(Files.notExists(errorsPath))
      dataStr = s"$logMessagePrefix failed with Cause:\n" ++ cause.prettyPrint
      data    = Chunk.fromArray(dataStr.getBytes(StandardCharsets.UTF_8))
      _      <- Files.writeBytes(filePath, data, StandardOpenOption.CREATE)
    } yield uuid
  }

  private def makePeerId: RIO[Random, Chunk[Byte]] = {
    for {
      peerHash <- random.nextBytes(12)
    } yield Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
  }

  private def messagesFromCause(cause: Cause[Throwable]): String = {
    cause.failures
      .map { throwable =>
        val message = throwable.getMessage
        if (message == null) ""
        else message.strip
      }.mkString(",")
  }

  private def configureLogging(): Unit = {
    val context      = LoggerFactory.getILoggerFactory.asInstanceOf[LoggerContext]
    val configurator = new JoranConfigurator()
    configurator.setContext(context)
    context.reset()
    configurator.doConfigure(getClass.getResourceAsStream("/logback.xml"))
  }
}
