package torr.fileio

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.nio.core._
import torr.channels._
import torr.directbuffers._
import torr.directbuffers.DirectBufferPool

import scala.annotation.tailrec

object Actor {

  sealed trait Command[+_]
  case object GetState                                       extends Command[State]
  case class Fetch(piece: Int, offset: Int)                  extends Command[ByteBuffer]
  case class Store(piece: Int, offset: Int, buf: ByteBuffer) extends Command[Unit]
  case class PieceHash(piece: Int)                           extends Command[Array[Byte]]
  case object GetNumAvailable                                extends Command[Int]

  // Multiple files in a torrent are treated like a single continuous file.
  case class File(offset: Long, size: Long, channel: SeekableByteChannel)
  case class State(pieceSize: Int, files: Vector[File])

  val stateful = new Stateful[Any, State, Command] {
    def receive[A](state: State, msg: Command[A], context: Context): RIO[Any, (State, A)] = {
      msg match {
        case GetState     => ZIO.succeed(state, state)
        case PieceHash(_) => ???
        case _            => ???
      }
    }
  }

  def pieceHash(state: State, piece: Int): Task[Array[Byte]] = {
    for {
      file <- ZIO(fileIndexOffset(piece, 0, state))
    } yield ???
  }

  private[fileio] def readMany(
      state: State,
      piece: Int,
      pieceOffset: Int,
      size: Int
  ): ZIO[DirectBufferPool, Throwable, Chunk[ByteBuffer]] = {

    val fileIndexOffset = for {
      _ <- ZIO.fail(new IllegalArgumentException(s"Size must be divisible by ${DirectBufferSize}"))
             .unless(size % DirectBufferSize == 0)
      //(fi, fo) <- ZIO(fileIndexOffset(piece, pieceOffset, state))
    } yield ???

    ???
  }

  //noinspection SimplifyZipRightToSucceedInspection
  private[fileio] def read(
      files: IndexedSeq[File],
      fileIndex: Int,
      fileOffset: Int,
      amount: Int,
      curBuf: Option[ByteBuffer] = None,
      acc: Chunk[ByteBuffer] = Chunk.empty
  ): ZIO[DirectBufferPool, Throwable, Chunk[ByteBuffer]] = {

    if (amount <= 0 || fileIndex >= files.length) {
      curBuf match {
        case Some(buf) =>
          for {
            _ <- buf.flip
          } yield acc :+ buf
        case None      => ZIO.succeed(acc)
      }
    } else {
      for {
        buf       <- curBuf.fold(DirectBufferPool.allocate)(ZIO(_))
        bytesRead <- files(fileIndex).channel.read(buf, fileOffset)
        remaining  = amount - bytesRead
        _         <- buf.moveLimit(remaining).when(remaining < 0) // We may have read more than requested
        fileSize   = files(fileIndex).size
        fileRem    = fileSize - (fileOffset + bytesRead)
        bufRem    <- buf.remaining
        res       <- (fileRem, bufRem) match {
                       // Not EOF, buffer is full
                       case (fr, 0) if fr > 0            =>
                         buf.flip *> read(files, fileIndex, fileOffset + bytesRead, remaining, None, acc :+ buf)
                       // EOF, buffer has free space
                       case (0, br) if br > 0            =>
                         read(files, fileIndex + 1, 0, remaining, Some(buf), acc)
                       // Not EOF, buffer has free space
                       case (fr, br) if fr > 0 && br > 0 =>
                         read(files, fileIndex, fileOffset + bytesRead, remaining, Some(buf), acc)
                       // EOF, Buffer is full
                       case (0, 0)                       =>
                         buf.flip *> read(files, fileIndex + 1, 0, remaining, None, acc :+ buf)
                     }
      } yield res
    }
  }

  /**
    * Translates piece index & piece offset into file index & file offset.
    */
  private[fileio] def fileIndexOffset(piece: Int, pieceOffset: Int, state: State): (Int, Long) = {

    // Effective offset in bytes
    val e = piece * state.pieceSize + pieceOffset

    // Binary search for file
    @tailrec
    def loop(i1: Int, i2: Int): Int = {
      val i = (i1 + i2) / 2
      state.files(i) match {
        case File(o, s, _) if o <= e && e < o + s => i
        case File(o, _, _) if e < o               => loop(i1, i)
        case File(o, s, _) if o + s <= e          => loop(i, i2)
        case _                                    =>
          throw new IllegalStateException(
            s"Could not find file by offset. Files: ${state.files.mkString(",")}; piece=$piece; offset=$pieceOffset"
          )
      }
    }

    val index  = loop(0, state.files.length)
    val offset = e - state.files(index).offset

    (index, offset)
  }
}
