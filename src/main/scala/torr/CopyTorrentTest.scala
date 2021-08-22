package torr

import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.channels.AsyncFileChannel
import torr.directbuffers.{DirectBufferPool, DirectBufferPoolLive}
import torr.fileio.Actor.{Fail, Fetch, GetState, Store}
import torr.fileio.Cache
import zio._
import torr.metainfo.MetaInfo
import zio.actors.ActorRef
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.logging.slf4j.Slf4jLogger
import zio.nio.core.{Buffer, ByteBuffer}
import zio.nio.core.channels.AsynchronousFileChannel
import zio.nio.core.file.Path
import zio.nio.file.Files

import java.nio.file.{OpenOption, StandardOpenOption}

object CopyTorrentTest extends App {

  val metaInfoFile     =
    "c:\\!temp\\Transcendence.2014.BDRip.1080.HDTracker.mkv.torrent"
  val blockSize        = 512 * 1024
  val srcDirectoryName = "d:\\Torrents\\"
  val dstDirectoryName = "c:\\!temp\\CopyTest\\"

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val effect = for {
      sys        <- ZIO.service[ActorSystem.Service].map(_.system)
      metaInfo   <- MetaInfo.fromFile(metaInfoFile)
      _          <- createFiles(metaInfo, dstDirectoryName)
      torrentSize = metaInfo.entries.map(_.size).sum

      actors = for {
                 srcFiles <- openFiles(metaInfo, srcDirectoryName, StandardOpenOption.READ)
                 dstFiles <- openFiles(metaInfo, dstDirectoryName, StandardOpenOption.WRITE)
                 srcActor <- createActor(
                               "Source",
                               fileio.Actor.State(metaInfo.pieceSize, torrentSize, Vector.from(srcFiles), Cache())
                             )
                 dstActor <- createActor(
                               "Destination",
                               fileio.Actor.State(metaInfo.pieceSize, torrentSize, Vector.from(dstFiles), Cache())
                             )
               } yield (srcActor, dstActor)

      _     <- actors.use {
                 case (src, dst) =>
                   copy(0, 0, metaInfo.pieces.length, metaInfo.pieceSize, torrentSize, src, dst)
               }

    } yield ()

    val actorSystem = ActorSystemLive.make("Test")
    val env         =
      (Clock.live ++ actorSystem ++ Slf4jLogger.make((_, message) => message)) >>>
        DirectBufferPoolLive.make(128) ++
          actorSystem

    effect.provideCustomLayer(env).exitCode
  }

  def createActor(
      name: String,
      state: fileio.Actor.State
  ): ZManaged[ActorSystem with DirectBufferPool with Clock with Console, Throwable, ActorRef[fileio.Actor.Command]] = {
    ZManaged.make {
      for {
        sys       <- ZIO.service[ActorSystem.Service].map(_.system)
        supervisor = actors.Supervisor.retryOrElse[DirectBufferPool, Unit](
                       Schedule.stop,
                       (throwable, _: Unit) => putStrLn(s"$throwable").provideLayer(Console.live).ignore
                     )
        res       <- sys.make(name, supervisor, state, fileio.Actor.stateful)
      } yield res
    }(a => a.stop.ignore)
  }

  def openFiles(
      metaInfo: MetaInfo,
      directoryName: String,
      options: OpenOption*
  ): ZManaged[Any, Throwable, List[fileio.Actor.OpenedFile]] = {

    def loop(
        entries: List[metainfo.FileEntry],
        offset: Long,
        acc: List[(metainfo.FileEntry, Long)] = Nil
    ): List[(metainfo.FileEntry, Long)] = {
      entries match {
        case Nil     => acc
        case e :: es => loop(es, offset + e.size, (e, offset) :: acc)
      }
    }

    val entriesOffsets = loop(metaInfo.entries, 0).reverse

    ZManaged.foreach(entriesOffsets) {
      case (entry, offset) =>
        AsyncFileChannel.open(Path(directoryName) / entry.path, options: _*)
          .map(channel => fileio.Actor.OpenedFile(offset, entry.size, channel))
    }
  }

  def createFiles(metaInfo: MetaInfo, directoryName: String): ZIO[Blocking, Throwable, Unit] = {
    ZIO.foreach_(metaInfo.entries) { entry =>
      for {
        buf  <- Buffer.byte(64 * 1024)
        path <- ZIO(Path(directoryName) / entry.path)
        _    <- createDirectory(path.parent.get)
        _    <- AsynchronousFileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                  .use(channel => fill(channel, entry.size, buf))
                  .whenM(Files.notExists(path))
      } yield ()
    }
  }

  def createDirectory(path: Path): ZIO[Blocking, Throwable, Unit] = {
    Files.exists(path).flatMap {
      case true  => ZIO.unit
      case false =>
        for {
          parent <- ZIO.fromOption(path.parent)
                      .orElseFail(new IllegalArgumentException("Path must be absolute"))
          _      <- createDirectory(parent) *> Files.createDirectory(path)
        } yield ()
    }
  }

  def fill(channel: AsynchronousFileChannel, amount: Long, buf: ByteBuffer, position: Long = 0): Task[Unit] = {
    amount - position match {
      case 0         => ZIO.unit
      case remaining =>
        for {
          _       <- buf.clear
          _       <- buf.limit(remaining.toInt).when(remaining < buf.capacity)
          written <- channel.write(buf, position)
          _       <- fill(channel, amount, buf, position + written)
        } yield ()
    }
  }

  def copy(
      piece: Int,
      offset: Int,
      pieces: Int,
      pieceLength: Int,
      torrentLength: Long,
      src: ActorRef[fileio.Actor.Command],
      dst: ActorRef[fileio.Actor.Command]
  ): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] = {

    val effectiveOffset = piece.toLong * pieceLength + offset

    //noinspection SimplifyUnlessInspection
    if (effectiveOffset >= torrentLength) {
      (dst ? GetState).as()
    } else {
      val readAmount = math.min(blockSize, torrentLength - effectiveOffset).toInt
      for {
        data <- src ? Fetch(piece, offset, readAmount)
        //data <- ZIO.foreach(Chunk.fill(4)())(_ => DirectBufferPool.allocate)
        _    <- dst ! Store(piece, offset, data)
        //_    <- ZIO.foreach_(data)(DirectBufferPool.free)
        _    <- printBuffersStats(effectiveOffset).when(effectiveOffset % (64 * 1024 * 1024) == 0L).fork
        _    <- (offset + readAmount) match {
                  case x if x == pieceLength =>
                    copy(piece + 1, 0, pieces, pieceLength, torrentLength, src, dst)
                  case _                     =>
                    copy(piece, offset + readAmount, pieces, pieceLength, torrentLength, src, dst)
                }
      } yield ()
    }
  }

  def printBuffersStats(position: Long): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] =
    for {
      avail <- DirectBufferPool.numAvailable
      _     <- putStrLn(s"${position / 1024 / 1024} - $avail")
    } yield ()

}
