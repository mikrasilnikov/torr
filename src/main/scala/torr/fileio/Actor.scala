package torr.fileio

import zio.{ZIO, _}
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.nio.core._
import torr.channels._
import torr.directbuffers._
import torr.directbuffers.DirectBufferPool
import zio.clock.Clock

import scala.annotation.tailrec

object Actor {

  sealed trait Command[+_]
  case object Fail                                                   extends Command[Unit]
  case object GetState                                               extends Command[State]
  case class Fetch(piece: Int, offset: Int, amount: Int)             extends Command[Chunk[ByteBuffer]]
  case class Store(piece: Int, offset: Int, data: Chunk[ByteBuffer]) extends Command[Unit]

  //case class CacheEntry(fileIndex : Int, fileOffset : Int, buf : ByteBuffer, )

  // Multiple files in a torrent are treated like a single continuous file.
  case class OpenedFile(offset: Long, size: Long, channel: SeekableByteChannel)
  case class State(pieceSize: Int, torrentSize: Long, files: Vector[OpenedFile])

  val stateful = new Stateful[DirectBufferPool with Clock, State, Command] {
    def receive[A](state: State, msg: Command[A], context: Context): RIO[DirectBufferPool with Clock, (State, A)] = {
      msg match {
        case GetState                     => ZIO.succeed(state, state)
        case Fetch(piece, offset, amount) => fetch(state, piece, offset, amount).map(b => (state, b))
        case Store(piece, offset, data)   => store(state, piece, offset, data).as(state, ())
        case Fail                         =>
          ZIO.fail(new Exception("Failed"))
      }
    }
  }

  private def fetch(
      state: State,
      piece: Int,
      offset: Int,
      amount: Int
  ): ZIO[DirectBufferPool with Clock, Throwable, Chunk[ByteBuffer]] = {
    val upTo = piece * state.pieceSize + offset + amount
    if (upTo > state.torrentSize)
      ZIO.fail(new IllegalArgumentException("Must not read beyond end of torrent"))
    else {
      val (fileIndex, fileOffset) = fileIndexOffset(state, piece, offset)
      read(state.files, fileIndex, fileOffset, amount)
    }
  }

  private def store(
      state: State,
      piece: Int,
      offset: Int,
      data: Chunk[ByteBuffer]
  ): ZIO[DirectBufferPool with Clock, Throwable, Unit] = {

    ZIO.foldLeft(data)(0)((acc, buf) => buf.remaining.map(acc + _))
      .flatMap { amount =>
        val upTo = piece * state.pieceSize + offset + amount
        if (upTo > state.torrentSize)
          ZIO.fail(new IllegalArgumentException("Must not write beyond end of torrent"))
        else {
          val (fileIndex, fileOffset) = fileIndexOffset(state, piece, offset)
          write(state.files, fileIndex, fileOffset, data)
        }
      }
  }

  /** Writes a chunk of buffers to torrent files starting at `files(index)` with specified `offset`. */
  private[fileio] def write(
      files: IndexedSeq[OpenedFile],
      fileIndex: Int,
      fileOffset: Long,
      data: Chunk[ByteBuffer]
  ): ZIO[DirectBufferPool with Clock, Throwable, Unit] = {
    data match {
      case Chunk.empty => ZIO.unit

      case _ =>
        for {
          (fileRem, bufRem) <- writeWithoutOverflow(files(fileIndex), fileOffset, data.head)

          newOffset = files(fileIndex).size - fileRem
          endOfFile = fileRem == 0
          endOfBuf  = bufRem == 0

          _ <- (endOfFile, endOfBuf) match {
                 case (false, true)  =>
                   DirectBufferPool.free(data.head) *>
                     write(files, fileIndex, newOffset, data.tail)
                 case (true, false)  => write(files, fileIndex + 1, 0, data)
                 case (false, false) => write(files, fileIndex, newOffset, data)
                 case (true, true)   =>
                   DirectBufferPool.free(data.head) *>
                     write(files, fileIndex + 1, 0, data.tail)
               }
        } yield ()
    }
  }

  /** Writes up to `file.size - offset` bytes to `file` from `buf` */
  private def writeWithoutOverflow(file: OpenedFile, offset: Long, buf: ByteBuffer): ZIO[Any, Throwable, (Long, Int)] =
    for {
      bufRem  <- buf.remaining
      fileRem  = file.size - offset
      dataFits = fileRem >= bufRem
      written <- if (dataFits)
                   file.channel.write(buf, offset)
                 else
                   for {
                     oldLimit    <- buf.limit
                     oldPosition <- buf.position
                     _           <- buf.limit(oldPosition + fileRem.toInt)
                     res         <- file.channel.write(buf, offset)
                     _           <- buf.limit(oldLimit)
                   } yield res
    } yield (fileRem - written, bufRem - written)

  /** Reads up to `amount` bytes from `files` into a chunk of buffers. Buffers are allocated from `DirectBufferPool`. */
  //noinspection SimplifyZipRightToSucceedInspection
  private[fileio] def read(
      files: IndexedSeq[OpenedFile],
      fileIndex: Int,
      fileOffset: Long,
      amount: Int,
      curBuf: Option[ByteBuffer] = None,
      acc: Chunk[ByteBuffer] = Chunk.empty
  ): ZIO[DirectBufferPool with Clock, Throwable, Chunk[ByteBuffer]] = {

    if (amount <= 0 || fileIndex >= files.length) {
      curBuf match {
        case Some(buf) => buf.flip *> ZIO.succeed(acc :+ buf)
        case None      => ZIO.succeed(acc)
      }
    } else {
      for {
        buf <- curBuf.fold(DirectBufferPool.allocate)(ZIO.succeed(_))
        rem <- readWithoutOverflow(files(fileIndex), fileOffset, amount, buf)

        (fileRem, bufRem) = rem

        newOffset = files(fileIndex).size - fileRem
        newAmount = amount - (newOffset - fileOffset).toInt

        endOfFile = fileRem == 0
        endOfBuf  = bufRem == 0

        res <- (endOfFile, endOfBuf) match {
                 case (false, true)  =>
                   buf.flip *> read(files, fileIndex, newOffset, newAmount, None, acc :+ buf)
                 case (true, false)  =>
                   read(files, fileIndex + 1, 0, newAmount, Some(buf), acc)
                 case (false, false) =>
                   read(files, fileIndex, newOffset, newAmount, Some(buf), acc)
                 case (true, true)   =>
                   buf.flip *> read(files, fileIndex + 1, 0, newAmount, None, acc :+ buf)
               }
      } yield res
    }
  }

  private def readWithoutOverflow(
      file: OpenedFile,
      offset: Long,
      amount: Int,
      buf: ByteBuffer
  ): ZIO[Any, Throwable, (Long, Int)] = {
    for {
      bytesRead <- file.channel.read(buf, offset)
      remaining  = amount - bytesRead
      _         <- buf.moveLimit(remaining).when(remaining < 0) // We may have read more than requested
      fileSize   = file.size
      fileRem    = fileSize - (offset + bytesRead)
      bufRem    <- buf.remaining
    } yield (fileRem, bufRem)
  }

  /**
    * Translates piece index & piece offset into file index & file offset.
    */
  private[fileio] def fileIndexOffset(state: State, piece: Int, pieceOffset: Int): (Int, Long) = {

    // Effective offset in bytes
    val e = piece.toLong * state.pieceSize + pieceOffset

    // Binary search for file
    @tailrec
    def loop(i1: Int, i2: Int): Int = {
      val i = (i1 + i2) / 2
      state.files(i) match {
        case OpenedFile(o, s, _) if o <= e && e < o + s => i
        case OpenedFile(o, _, _) if e < o               => loop(i1, i)
        case OpenedFile(o, s, _) if o + s <= e          => loop(i, i2)
        case _                                          =>
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
