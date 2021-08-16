package torr.fileio

import zio.{RIO, ZIO, _}
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.nio.core._
import torr.channels._
import torr.directbuffers._
import torr.directbuffers.DirectBufferPool
import zio.clock.Clock

import scala.annotation.tailrec

object Actor {

  private val CacheEntrySize: Long = 128 * 1024

  sealed trait Command[+_]
  case object Fail                                                        extends Command[Unit]
  case object GetState                                                    extends Command[State]
  case class Fetch(piece: Int, pieceOffset: Int, amount: Long)            extends Command[Chunk[ByteBuffer]]
  case class Store(piece: Int, pieceOffset: Int, data: Chunk[ByteBuffer]) extends Command[Unit]

  case class EntryAddr(fileIndex: Int, entryIndex: Long) // offset = entryIndex * CacheEntrySize

  case class Range(from: Long, until: Long) {
    val length: Long = until - from
  }

  sealed trait CacheEntry { def addr: EntryAddr; def data: ByteBuffer }
  case class ReadEntry(addr: EntryAddr, data: ByteBuffer)  extends CacheEntry
  case class WriteEntry(addr: EntryAddr, data: ByteBuffer) extends CacheEntry {
    def isFull: Boolean = ???
  }

  sealed trait LookupResult
  case class Hit(entry: CacheEntry, entryRange: Range)     extends LookupResult
  case class Miss(entryAddr: EntryAddr, entryRange: Range) extends LookupResult

  // Multiple files in a torrent are treated like a single continuous file.
  case class OpenedFile(offset: Long, size: Long, channel: SeekableByteChannel)

  case class State(
      pieceSize: Int,
      torrentSize: Long,
      files: Vector[OpenedFile],
      var cacheHits: Long = 0,
      var cacheMisses: Long = 0
  )

  val stateful = new Stateful[DirectBufferPool with Clock, State, Command] {

    def receive[A](state: State, msg: Command[A], context: Context): RIO[DirectBufferPool with Clock, (State, A)] = {
      msg match {

        case Fetch(piece, pieceOffset, amount) =>
          for {
            absolute <- ZIO(absoluteOffset(state, piece, pieceOffset))
            _        <- validateAmount(state, absolute, amount)
            result   <- read(state, absolute, amount)
          } yield (state, result)

        case Store(piece, pieceOffset, data)   =>
          for {
            absolute <- ZIO(absoluteOffset(state, piece, pieceOffset))
            amount   <- ZIO.foldLeft(data)(0L) { case (acc, buf) => buf.remaining.map(acc + _) }
            _        <- validateAmount(state, absolute, amount)
            _        <- write(state, absolute, data)
          } yield (state, ())

        case GetState                          => ZIO.succeed(state, state)
        case Fail                              => ZIO.fail(new Exception("Failed"))
      }
    }
  }

  private[fileio] def read(
      state: State,
      offset: Long,
      amount: Long,
      acc: Chunk[ByteBuffer] = Chunk.empty
  ): RIO[DirectBufferPool with Clock, Chunk[ByteBuffer]] =
    if (amount <= 0) ZIO.succeed(acc)
    else
      for {
        buf           <- DirectBufferPool.allocate
        lookupResults <- cacheLookup(state, offset, buf)
        bytesRead     <- cachedReads(state, lookupResults, buf)
        res           <- read(state, offset + bytesRead, amount - bytesRead, acc :+ buf)
      } yield res

  private[fileio] def cachedReads(
      state: State,
      lookupResults: Chunk[LookupResult],
      buf: ByteBuffer,
      acc: Long = 0L
  ): RIO[DirectBufferPool with Clock, Long] = {
    if (lookupResults.isEmpty) ZIO.succeed(acc)
    else
      for {
        bytesRead <- cachedRead(state, lookupResults.head, buf)
        result    <- cachedReads(state, lookupResults.tail, buf, acc + bytesRead)
      } yield result
  }

  private[fileio] def cachedRead(state: State, lookupResult: LookupResult, buf: ByteBuffer): Task[Long] = {
    lookupResult match {
      case Hit(entry @ ReadEntry(_, _), entryRange)  =>
        for {
          _      <- ZIO(markCacheHit(state))
          copied <- copyToBufAndMarkEntryUse(entry, entryRange, buf)
        } yield copied

      case Hit(entry @ WriteEntry(_, _), entryRange) =>
        for {
          _         <- ZIO(markCacheHit(state))
          readEntry <- makeReadEntryFromWriteEntry(entry)
          copied    <- copyToBufAndMarkEntryUse(readEntry, entryRange, buf)
        } yield copied

      case Miss(addr, entryRange)                    =>
        for {
          _      <- ZIO(markCacheMiss(state))
          entry  <- makeReadEntry(addr)
          copied <- copyToBufAndMarkEntryUse(entry, entryRange, buf)
        } yield copied
    }
  }

  //noinspection SimplifyUnlessInspection
  private[fileio] def write(
      state: State,
      offset: Long,
      data: Chunk[ByteBuffer]
  ): RIO[DirectBufferPool, Unit] = {
    if (data.isEmpty) ZIO.unit
    else
      for {
        lookupResults <- cacheLookup(state, offset, data.head)
        bytesWritten  <- cachedWrites(state, lookupResults, data.head)
        _             <- write(state, offset + bytesWritten, data.tail)
      } yield ()
  }

  private[fileio] def cachedWrites(
      state: State,
      lookupResults: Chunk[LookupResult],
      buf: ByteBuffer,
      acc: Long = 0
  ): RIO[DirectBufferPool, Long] = {
    if (lookupResults.isEmpty) ZIO.succeed(acc)
    else
      for {
        written <- cachedWrite(state, lookupResults.head, buf)
        result  <- cachedWrites(state, lookupResults.tail, buf, acc + written)
      } yield result
  }

  private[fileio] def cachedWrite(
      state: State,
      lookupResult: LookupResult,
      buf: ByteBuffer
  ): RIO[DirectBufferPool, Long] =
    lookupResult match {
      case Hit(entry @ ReadEntry(_, _), entryRange)  =>
        for {
          _        <- ZIO(markCacheHit(state))
          newEntry <- makeWriteEntryFromReadEntry(entry)
          copied   <- copyToEntryAndScheduleWriteout(newEntry, entryRange, buf)
          _        <- DirectBufferPool.free(buf)
        } yield copied

      case Hit(entry @ WriteEntry(_, _), entryRange) =>
        for {
          _      <- ZIO(markCacheHit(state))
          copied <- copyToEntryAndScheduleWriteout(entry, entryRange, buf)
          _      <- DirectBufferPool.free(buf)
        } yield copied

      case Miss(addr, entryRange)                    =>
        for {
          _      <- ZIO(markCacheMiss(state))
          entry  <- makeWriteEntry(addr)
          copied <- copyToEntryAndScheduleWriteout(entry, entryRange, buf)
          _      <- DirectBufferPool.free(buf)
        } yield copied
    }

  private def markCacheHit(state: State): Unit  = state.cacheHits = state.cacheHits + 1
  private def markCacheMiss(state: State): Unit = state.cacheMisses = state.cacheMisses + 1

  private def makeReadEntry(addr: EntryAddr): Task[ReadEntry]        = ???
  private def makeWriteEntry(entryAddr: EntryAddr): Task[WriteEntry] = ???

  private[fileio] def makeReadEntryFromWriteEntry(entry: WriteEntry): Task[ReadEntry] = ???
  private[fileio] def makeWriteEntryFromReadEntry(entry: ReadEntry): Task[WriteEntry] = ???

  private def copyToEntryAndScheduleWriteout(
      entry: WriteEntry,
      entryRange: Range,
      buf: ByteBuffer
  ): Task[Long] = ???

  private def copyToBufAndMarkEntryUse(
      entry: ReadEntry,
      entryRange: Range,
      buf: ByteBuffer
  ): Task[Long] = ???

  private def scheduleWriteOut(entry: WriteEntry): Task[Unit] = ???

  private def writeOut(entry: WriteEntry): Task[Unit] = ???

  private[fileio] def cacheLookup(
      state: State,
      offset: Long,
      buf: ByteBuffer
  ): RIO[DirectBufferPool, Chunk[LookupResult]] = ???

  private[fileio] def absoluteOffset(state: State, piece: Int, pieceOffset: Int): Long             = ???
  private[fileio] def validateAmount(state: State, absoluteOffset: Long, amount: Long): Task[Unit] = ???

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

  def read(files: Chunk[OpenedFile], fileIndex: Int, fileOffset: Int, amount: Int): Task[Chunk[ByteBuffer]]  = ???
  def write(files: Chunk[OpenedFile], fileIndex: Int, fileOffset: Int, value: Chunk[ByteBuffer]): Task[Unit] = ???
}
