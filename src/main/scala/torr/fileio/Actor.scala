package torr.fileio

import zio.{ZIO, _}
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.nio.core._
import torr.channels._
import torr.directbuffers._
import torr.directbuffers.DirectBufferPool

import scala.annotation.tailrec

object Actor {

  val MinimumTorrentBlockSize: Int = 16 * 1024

  sealed trait Command[+_]
  case object GetState                                               extends Command[State]
  case class Fetch(piece: Int, offset: Int, amount: Int)             extends Command[Chunk[ByteBuffer]]
  case class Store(piece: Int, offset: Int, data: Chunk[ByteBuffer]) extends Command[Unit]

  // Multiple files in a torrent are treated like a single continuous file.
  case class File(offset: Long, size: Long, channel: SeekableByteChannel)
  case class State(pieceSize: Int, torrentSize: Long, files: Vector[File])

  val stateful = new Stateful[DirectBufferPool, State, Command] {
    def receive[A](state: State, msg: Command[A], context: Context): RIO[DirectBufferPool, (State, A)] = {
      msg match {
        case GetState                     => ZIO.succeed(state, state)
        case Fetch(piece, offset, amount) => fetch(state, piece, offset, amount).map(b => (state, b))
        case Store(piece, offset, data)   => store(state, piece, offset, data).as(state, ())
      }
    }
  }

  private def fetch(
      state: State,
      piece: Int,
      offset: Int,
      amount: Int
  ): ZIO[DirectBufferPool, Throwable, Chunk[ByteBuffer]] = {
    val upTo = piece * state.pieceSize + offset + amount
    if (upTo > state.torrentSize) ZIO.fail(new IllegalArgumentException("Must not read beyond end of torrent"))
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
  ): ZIO[DirectBufferPool, Throwable, Unit] = {

    ZIO.foldLeft(data)(0)((acc, buf) => buf.remaining.map(acc + _))
      .flatMap { amount =>
        val upTo = piece * state.pieceSize + offset + amount
        if (upTo > state.torrentSize) ZIO.fail(new IllegalArgumentException("Must not write beyond end of torrent"))
        else {
          val (fileIndex, fileOffset) = fileIndexOffset(state, piece, offset)
          write(state.files, fileIndex, fileOffset, data)
        }

        val (fileIndex, fileOffset) = fileIndexOffset(state, piece, offset)
        write(state.files, fileIndex, fileOffset, data)
      }

  }

  /** Writes a chunk of buffers to torrent files starting at `files(fileIndex)` with specified `fileOffset`. */
  private[fileio] def write(
      files: IndexedSeq[File],
      fileIndex: Int,
      fileOffset: Long,
      data: Chunk[ByteBuffer]
  ): ZIO[DirectBufferPool, Throwable, Unit] = {
    data match {
      case Chunk.empty => ZIO.unit
      case _           =>
        for {
          bufRem <- data.head.remaining
          fileRem = files(fileIndex).size - fileOffset
          _      <- ZIO(assert(bufRem > 0))
          _      <- ZIO(assert(fileRem > 0))

          written <- if (fileRem >= bufRem)
                       files(fileIndex).channel.write(data.head, fileOffset)
                     else {
                       val managedBuf = ZManaged.make(DirectBufferPool.allocate)(b => DirectBufferPool.free(b).ignore)
                       managedBuf.use { buf =>
                         for {
                           chunk <- data.head.getChunk(math.min(fileRem.toInt, buf.capacity))
                           _     <- buf.putChunk(chunk) *> buf.flip
                           res   <- files(fileIndex).channel.write(buf, fileOffset)
                         } yield res
                       }
                     }

          _       <- (fileRem - written, bufRem - written) match {
                       // Not EOF, buffer is empty
                       case (fr, 0) if fr > 0            => write(files, fileIndex, fileOffset + written, data.tail)
                       // EOF, more data available
                       case (0, dr) if dr > 0            => write(files, fileIndex + 1, 0, data)
                       // Not EOF, more data available
                       case (fr, dr) if fr > 0 && dr > 0 => write(files, fileIndex, fileOffset + written, data)
                       // EOF, buffer is empty
                       case (0, 0)                       => write(files, fileIndex + 1, 0, data.tail)
                     }
        } yield ()
    }
  }

  /** Reads up to `amount` bytes from `files` into a chunk of buffers. Buffers are allocated from `DirectBufferPool`. */
  //noinspection SimplifyZipRightToSucceedInspection
  private[fileio] def read(
      files: IndexedSeq[File],
      fileIndex: Int,
      fileOffset: Long,
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
  private[fileio] def fileIndexOffset(state: State, piece: Int, pieceOffset: Int): (Int, Long) = {

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
