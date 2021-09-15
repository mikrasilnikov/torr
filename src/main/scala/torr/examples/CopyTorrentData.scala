package torr.examples

import torr.actorsystem._
import torr.channels.AsyncFileChannel
import torr.directbuffers.{DirectBufferPool, DirectBufferPoolLive}
import torr.fileio.Actor._
import torr.fileio._
import torr.metainfo._
import zio.actors.ActorRef
import zio.blocking.Blocking
import zio.clock.Clock
import zio.console._
import zio.logging.slf4j.Slf4jLogger
import zio.nio.core.channels.AsynchronousFileChannel
import zio.nio.core.file.Path
import zio.nio.core._
import zio.nio.file.Files
import zio._
import zio.duration.durationInt

import java.nio.file._

object CopyTorrentData extends App {

  val metaInfoFile     =
    "d:\\Torrents\\!torrent\\Ragdoll_Masters v3.1.torrent"
  val srcDirectoryName = "c:\\!temp\\CopyTest\\"
  val dstDirectoryName = "c:\\!temp\\CopyTest1\\"

  val blockSize: Int = 32 * 1024

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val effect = for {
      sys        <- ZIO.service[ActorSystem.Service].map(_.system)
      metaInfo   <- MetaInfo.fromFile(metaInfoFile)
      _          <- createFiles(metaInfo, dstDirectoryName)
      torrentSize = metaInfo.entries.map(_.size).sum

      actors = for {
                 srcFiles <- openFiles(metaInfo, srcDirectoryName, StandardOpenOption.READ)
                 dstFiles <- openFiles(metaInfo, dstDirectoryName, StandardOpenOption.READ, StandardOpenOption.WRITE)
                 srcActor <- createActor(
                               "Source",
                               Actor.State(
                                 metaInfo.pieceSize,
                                 torrentSize,
                                 Vector.from(srcFiles),
                                 Cache(cacheEntriesNum = 16, entrySize = 512 * 1024)
                               )
                             )
                 dstActor <- createActor(
                               "Destination",
                               Actor.State(
                                 metaInfo.pieceSize,
                                 torrentSize,
                                 Vector.from(dstFiles),
                                 Cache(cacheEntriesNum = 16, entrySize = 512 * 1024)
                               )
                             )
               } yield (srcActor, dstActor)

      _     <- actors.use {
                 case (src, dst) =>
                   //copySequential(0, 0, metaInfo.pieces.length, metaInfo.pieceSize, torrentSize, src, dst)
                   copyRandom(metaInfo.pieceSize, torrentSize, src, dst)
               }

    } yield ()

    val actorSystem = ActorSystemLive.make("Test")
    val env         =
      (Clock.live ++ actorSystem ++ Slf4jLogger.make((_, message) => message)) >>>
        DirectBufferPoolLive.make(1, 32 * 1024) ++
          actorSystem

    effect.provideCustomLayer(env).exitCode
  }

  def createActor(
      name: String,
      state: Actor.State
  ): ZManaged[ActorSystem with DirectBufferPool with Clock with Console, Throwable, ActorRef[Actor.Command]] = {
    ZManaged.make {
      for {
        sys       <- ZIO.service[ActorSystem.Service].map(_.system)
        supervisor = actors.Supervisor.retryOrElse[DirectBufferPool, Unit](
                       Schedule.stop,
                       (throwable, _: Unit) => putStrLn(s"${throwable.toString}").provideLayer(Console.live).ignore
                     )
        res       <- sys.make(name, supervisor, state, Actor.stateful)
      } yield res
    }(a => a.stop.ignore)
  }

  def openFiles(
      metaInfo: MetaInfo,
      directoryName: String,
      options: OpenOption*
  ): ZManaged[Any, Throwable, List[Actor.OpenedFile]] = {

    def loop(
        entries: List[torr.metainfo.FileEntry],
        offset: Long,
        acc: List[(torr.metainfo.FileEntry, Long)] = Nil
    ): List[(torr.metainfo.FileEntry, Long)] = {
      entries match {
        case Nil     => acc
        case e :: es => loop(es, offset + e.size, (e, offset) :: acc)
      }
    }

    val entriesOffsets = loop(metaInfo.entries, 0).reverse

    ZManaged.foreach(entriesOffsets) {
      case (entry, offset) =>
        AsyncFileChannel.open(Path(directoryName) / entry.path, options: _*)
          .map(channel => Actor.OpenedFile(offset, entry.size, channel))
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

  def copySequential(
      piece: Int,
      offset: Int,
      pieces: Int,
      pieceLength: Int,
      torrentLength: Long,
      src: ActorRef[Actor.Command],
      dst: ActorRef[Actor.Command]
  ): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] = {

    val effectiveOffset = piece.toLong * pieceLength + offset

    //noinspection SimplifyUnlessInspection
    if (effectiveOffset >= torrentLength) {
      (dst ? GetState).as()
    } else {
      val readAmount = math.min(blockSize, torrentLength - effectiveOffset).toInt
      for {
        data <- src ? Fetch(piece, offset, readAmount)
        _    <- dst ! Store(piece, offset, data)
        _    <- printBuffersStats(effectiveOffset).when(effectiveOffset % (64 * 1024 * 1024) == 0L).fork
        _    <- (offset + readAmount) match {
                  case x if x == pieceLength =>
                    copySequential(piece + 1, 0, pieces, pieceLength, torrentLength, src, dst)
                  case _                     =>
                    copySequential(piece, offset + readAmount, pieces, pieceLength, torrentLength, src, dst)
                }
      } yield ()
    }
  }

  def copyRandom(
      pieceSize: Int,
      torrentLength: Long,
      src: ActorRef[Actor.Command],
      dst: ActorRef[Actor.Command]
  ): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] = {
    val rnd      = new java.util.Random(43)
    val bufCount = (torrentLength / blockSize).toInt
    val bufOrder = (0 to bufCount).sortBy(_ => rnd.nextInt()).toList

    def copy(
        bufNums: List[Int],
        srcState: State,
        dstState: State,
        bufsWritten: Int = 0
    ): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] =
      bufNums match {
        case Nil     => ZIO.unit
        case n :: ns =>
          val offset      = n.toLong * blockSize
          val piece       = (offset / pieceSize).toInt
          val pieceOffset = (offset % pieceSize).toInt
          val amount      = math.min(blockSize, torrentLength - offset).toInt

          for {
            data <- src ? Fetch(piece, pieceOffset, amount)
            rem  <- ZIO.foldLeft(data)(0L) { case (acc, buf) => buf.remaining.map(acc + _) }
            _    <- putStrLn(s"before Store rem = $rem").when(rem != 32768)
            _    <- dst ! Store(piece, pieceOffset, data)
            _    <- putStrLn(s"$bufsWritten of $bufCount completed").when(bufsWritten % 1000 == 0)
            _    <- copy(ns, srcState, dstState, bufsWritten + 1)
          } yield ()
      }

    for {
      srcState <- src ? GetState
      dstState <- dst ? GetState
      _        <- copy(bufOrder, srcState, dstState)
      _        <- (dst ? GetState).debug
      _        <- ZIO.sleep(40.seconds)
      _        <- (dst ? GetState).debug
    } yield ()

  }

  def printBuffersStats(position: Long): ZIO[Console with Clock with DirectBufferPool, Throwable, Unit] =
    for {
      avail <- DirectBufferPool.numAvailable
      _     <- putStrLn(s"${position / 1024 / 1024} - $avail")
    } yield ()

}
