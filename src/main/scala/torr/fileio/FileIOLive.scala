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
import torr.fileio.Actor.{Command, Fetch, GetState, State, Store}
import torr.metainfo.MetaInfo

import java.nio.file.{OpenOption, StandardOpenOption}

case class FileIOLive(private val actor: ActorRef[Command]) extends FileIO.Service {

  def fetch(piece: Int, offset: Int, amount: Int): ZManaged[DirectBufferPool, Throwable, Chunk[ByteBuffer]] = {
    val read = actor ? Fetch(piece, offset, amount)
    read.toManaged(bufs => ZIO.foreach_(bufs)(b => DirectBufferPool.free(b).orDie))
  }

  def store(piece: Int, offset: Int, data: Chunk[ByteBuffer]): Task[Unit] = actor ! Store(piece, offset, data)

  def hash(piece: Int): Task[Array[Byte]] = ???
}

object FileIOLive {

  def make(
      metaInfo: MetaInfo,
      baseDirectoryName: String
  ): ZLayer[ActorSystem with DirectBufferPool with Clock, Throwable, FileIO] = {

    val managed = for {
      files      <- openFiles(metaInfo, baseDirectoryName, StandardOpenOption.READ, StandardOpenOption.WRITE)
      torrentSize = metaInfo.entries.map(_.size).sum
      state       = State(metaInfo.pieceSize, torrentSize, files.toVector, Cache())
      actor      <- createActor("FileIO", state)
    } yield FileIOLive(actor)

    managed.toLayer
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

  def createActor(
      name: String,
      state: fileio.Actor.State
  ): ZManaged[ActorSystem with DirectBufferPool with Clock, Throwable, ActorRef[Command]] = {

    val make = for {
      sys       <- ZIO.service[ActorSystem.Service].map(_.system)
      supervisor = actors.Supervisor.retryOrElse[DirectBufferPool, Unit](
                     Schedule.stop,
                     (throwable, _: Unit) => ZIO.die(throwable)
                   )
      res       <- sys.make(name, supervisor, state, fileio.Actor.stateful)
    } yield res

    make.toManaged { actorRef =>
      val stop = for {
        _ <- actorRef ? GetState
        _ <- actorRef.stop.ignore
      } yield ()
      stop.ignore
    }
  }
}
