package torr.fileio

import zio._
import zio.clock.Clock
import zio.actors._
import zio.nio.core._
import zio.nio.core.file.Path
import torr.actorsystem.ActorSystem
import torr.channels.AsyncFileChannel
import torr.consoleui.SimpleProgressBar
import torr.directbuffers.DirectBufferPool
import torr.{fileio, metainfo}
import torr.fileio.Actor.{Command, Fetch, Flush, GetState, State, Store}
import torr.metainfo.MetaInfo
import zio.blocking.Blocking
import zio.console.{Console, putStr, putStrLn}
import zio.duration.durationInt
import zio.nio.file.Files

import java.nio.file.{LinkOption, OpenOption, StandardOpenOption}

case class FileIOLive(private val actor: ActorRef[Command], mi: MetaInfo, freshFiles: Boolean) extends FileIO.Service {

  def fetch(piece: Int, offset: Int, amount: Int): ZIO[DirectBufferPool, Throwable, Chunk[ByteBuffer]] = {
    actor ? Fetch(piece, offset, amount)
  }

  def store(piece: Int, offset: Int, data: Chunk[ByteBuffer]): Task[Unit] = {
    actor ! Store(piece, offset, data)
  }

  def flush: Task[Unit] = {
    actor ? Flush
  }

  def metaInfo: Task[MetaInfo] = ZIO.succeed(mi)

  def freshFilesWereAllocated: Task[Boolean] = ZIO.succeed(freshFiles)
}

object FileIOLive {

  //noinspection EmptyCheck, OptionEqualsSome
  def make(
      metaInfoFileName: String,
      baseDirectoryName: String,
      allocateFiles: Boolean = true,
      actorName: String = "FileIO"
  ): ZLayer[ActorSystem with DirectBufferPool with Console with Clock with Blocking, Throwable, FileIO] = {

    val managed = for {
      metaInfo                <- MetaInfo.fromFile(metaInfoFileName).toManaged_
      sizeOptions             <- getFileSizes(metaInfo, baseDirectoryName).toManaged_
      allFilesDontExist        = sizeOptions.forall(_ == None)
      allFilesHaveCorrectSizes = sizesAreEqual(sizeOptions, metaInfo.entries.map(_.size))
      files                   <- if (allFilesDontExist && allocateFiles)
                                   createFiles(metaInfo, baseDirectoryName).toManaged_ *>
                                     openFiles(metaInfo, baseDirectoryName, StandardOpenOption.READ, StandardOpenOption.WRITE)
                                 else if (allFilesHaveCorrectSizes)
                                   openFiles(metaInfo, baseDirectoryName, StandardOpenOption.READ, StandardOpenOption.WRITE)
                                 else
                                   ZManaged.fail(new Exception(
                                     "Contents of the target directory is corrupted. Please remove all files from " +
                                       "the target directory and restart download."
                                   ))

      torrentSize              = metaInfo.entries.map(_.size).sum
      state                    = State(metaInfo.pieceSize, torrentSize, files.toVector, Cache())
      actor                   <- createActorManaged(actorName, state)
    } yield FileIOLive(actor, metaInfo, allFilesDontExist)

    managed.toLayer
  }

  private def getFileSizes(metaInfo: MetaInfo, directoryName: String): RIO[Blocking, List[Option[Long]]] = {
    ZIO.foreach(metaInfo.entries) { entry =>
      for {
        path   <- ZIO(Path(directoryName) / entry.path)
        exists <- Files.exists(path)
        res    <- if (exists) Files.size(path).map(Some(_))
                  else ZIO.none
      } yield res
    }
  }

  private def sizesAreEqual(sizeOptions1: List[Option[Long]], sizes2: List[Long]): Boolean = {
    sizeOptions1.size == sizes2.size &&
    (sizeOptions1 zip sizes2).forall {
      case (Some(s1), s2) => s1 == s2
      case _              => false
    }
  }

  private def createFiles(
      metaInfo: MetaInfo,
      directoryName: String
  ): RIO[Console with Clock with Blocking, Unit] = {
    for {
      refWritten  <- Ref.make[Long](0)
      progressFib <- (updateProgress(refWritten, metaInfo.torrentSize) *> ZIO.sleep(1.second)).forever.fork
      _           <- ZIO.foreach_(metaInfo.entries) { entry =>
                       for {
                         buf  <- Buffer.byteDirect(256 * 1024)
                         path <- ZIO(Path(directoryName) / entry.path)
                         _    <- createDirectory(path.parent.get)
                         _    <- AsyncFileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                                   .use(channel => fillFile(channel, entry.size, buf, refWritten))
                       } yield ()
                     }
      _           <- progressFib.interrupt.delay(1.second) *> putStrLn("")
    } yield ()

  }

  private def updateProgress(refWritten: Ref[Long], total: Long): RIO[Console, Unit] = {
    for {
      written <- refWritten.get
      progress = written * 100 / total
      output   = SimpleProgressBar.render("Allocating files", 50, progress.toInt)
      _       <- (putStr("\b" * output.length) *> putStr(output)).uninterruptible
    } yield ()
  }

  private def createDirectory(path: Path): ZIO[Blocking, Throwable, Unit] = {
    Files.isDirectory(path).flatMap {
      case true  => ZIO.unit
      case false =>
        for {
          parent <- ZIO.fromOption(path.parent)
                      .orElseFail(new IllegalArgumentException("Path must be absolute"))
          _      <- createDirectory(parent) *> Files.createDirectory(path)
        } yield ()
    }
  }

  private def fillFile(
      channel: AsyncFileChannel,
      amount: Long,
      buf: ByteBuffer,
      totalWritten: Ref[Long],
      position: Long = 0
  ): Task[Unit] = {
    amount - position match {
      case 0         => ZIO.unit
      case remaining =>
        for {
          _       <- buf.clear
          _       <- buf.limit(remaining.toInt).when(remaining < buf.capacity)
          written <- channel.write(buf, position)
          _       <- totalWritten.modify(old => ((), old + written))
          _       <- fillFile(channel, amount, buf, totalWritten, position + written)
        } yield ()
    }
  }

  private def openFiles(
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

  private def createActor(
      name: String,
      state: fileio.Actor.State
  ): ZIO[ActorSystem with DirectBufferPool with Clock, Throwable, ActorRef[Command]] = {
    for {
      sys       <- ZIO.service[ActorSystem.Service].map(_.system)
      supervisor = actors.Supervisor.retryOrElse[Any, Unit](
                     Schedule.stop,
                     (throwable, _: Unit) => ZIO.die(throwable)
                   )
      res       <- sys.make(name, supervisor, state, fileio.Actor.stateful)
    } yield res
  }

  private[fileio] def createActorManaged(
      name: String,
      state: fileio.Actor.State
  ): ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, ActorRef[Command]] = {

    createActor(name, state).toManaged { actorRef =>
      val stop = for {
        _ <- actorRef ? GetState
        _ <- actorRef.stop.ignore
      } yield ()
      stop.ignore
    }
  }
}
