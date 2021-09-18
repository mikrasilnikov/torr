package torr.fileio

import zio._
import zio.clock.Clock
import zio.actors._
import zio.nio.core._
import zio.nio.core.file.Path
import torr.actorsystem.ActorSystem
import torr.channels.AsyncFileChannel
import torr.directbuffers.DirectBufferPool
import torr.{fileio, metainfo}
import torr.fileio.Actor.{Command, Fetch, Flush, GetState, State, Store}
import torr.metainfo.MetaInfo
import zio.blocking.Blocking
import zio.nio.file.Files

import java.nio.file.{OpenOption, StandardOpenOption}

case class FileIOLive(private val actor: ActorRef[Command], metaInfo: MetaInfo) extends FileIO.Service {

  def fetch(piece: Int, offset: Int, amount: Int): ZIO[DirectBufferPool, Throwable, Chunk[ByteBuffer]] = {
    actor ? Fetch(piece, offset, amount)
  }

  def store(piece: Int, offset: Int, data: Chunk[ByteBuffer]): Task[Unit] = {
    actor ! Store(piece, offset, data)
  }

  def flush: Task[Unit] = {
    actor ? Flush
  }

}

object FileIOLive {

  def make(
      metaInfoFileName: String,
      baseDirectoryName: String,
      actorName: String = "FileIO"
  ): ZLayer[ActorSystem with DirectBufferPool with Clock with Blocking, Throwable, FileIO] = {

    val managed = for {
      metaInfo   <- MetaInfo.fromFile(metaInfoFileName).toManaged_
      _          <- createFiles(metaInfo, baseDirectoryName).toManaged_
      files      <- openFiles(metaInfo, baseDirectoryName, StandardOpenOption.READ, StandardOpenOption.WRITE)
      torrentSize = metaInfo.entries.map(_.size).sum
      state       = State(metaInfo.pieceSize, torrentSize, files.toVector, Cache())
      actor      <- createActorManaged(actorName, state)
    } yield FileIOLive(actor, metaInfo)

    managed.toLayer
  }

  private def createFiles(metaInfo: MetaInfo, directoryName: String): ZIO[Blocking, Throwable, Unit] = {
    ZIO.foreach_(metaInfo.entries) { entry =>
      for {
        buf  <- Buffer.byte(64 * 1024)
        path <- ZIO(Path(directoryName) / entry.path)
        _    <- createDirectory(path.parent.get)
        _    <- AsyncFileChannel.open(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)
                  .use(channel => fill(channel, entry.size, buf))
                  .whenM(Files.notExists(path))
      } yield ()
    }
  }

  private def createDirectory(path: Path): ZIO[Blocking, Throwable, Unit] = {
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

  private def fill(channel: AsyncFileChannel, amount: Long, buf: ByteBuffer, position: Long = 0): Task[Unit] = {
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
