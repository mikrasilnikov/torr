package torr.fileio

import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context
import zio.nio.core._
import torr.channels._
import torr.directbuffers.DirectBufferPool
import zio.clock.Clock
import zio.duration.{Duration, durationInt}

import scala.collection.mutable
import scala.annotation.tailrec

object Actor {

  val MaxCacheEntries: Int    = 100
  val CacheEntrySize: Int     = 128 * 1024
  val WriteOutDelay: Duration = 30.seconds

  sealed trait Command[+_]
  case object Fail                                                        extends Command[Unit]
  case object GetState                                                    extends Command[State]
  case class Fetch(piece: Int, pieceOffset: Int, amount: Long)            extends Command[Chunk[ByteBuffer]]
  case class Store(piece: Int, pieceOffset: Int, data: Chunk[ByteBuffer]) extends Command[Unit]

  private[fileio] case class SynchronizeAndWriteOut(writeEntry: WriteEntry) extends Command[Unit]

  sealed trait LookupResult
  case class Hit(entry: CacheEntry, entryRange: IntRange)     extends LookupResult
  case class Miss(entryAddr: EntryAddr, entryRange: IntRange) extends LookupResult

  case class LRUEntry(relevanceOnEnqueue: Long, cacheEntry: CacheEntry)

  // Multiple files in a torrent are treated like a single continuous file.
  case class OpenedFile(offset: Long, size: Long, channel: SeekableByteChannel)

  case class State(
      pieceSize: Int,
      torrentSize: Long,
      files: Vector[OpenedFile],
      cacheEntries: mutable.HashMap[EntryAddr, CacheEntry] = new mutable.HashMap[EntryAddr, CacheEntry],
      cacheRelevance: mutable.Queue[LRUEntry] = new mutable.Queue[LRUEntry],
      var currentRelevance: Long = 0,
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
            _        <- write(context, state, absolute, data)
          } yield (state, ())

        case GetState                          => ZIO.succeed(state, state)
        case Fail                              => ZIO.fail(new Exception("Failed"))

        case SynchronizeAndWriteOut(writeEntry) => ???
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
      case Hit(entry @ ReadEntry(_, _, _), entryRange)  =>
        for {
          _      <- ZIO(markCacheHit(state))
          copied <- entry.read(buf, entryRange.from.toInt, entryRange.length.toInt)
          _       = markEntryUse(state, entry)
        } yield copied

      case Hit(entry @ WriteEntry(_, _, _), entryRange) =>
        for {
          _         <- ZIO(markCacheHit(state))
          readEntry <- makeReadEntryFromWriteEntry(state, entry)
          copied    <- readEntry.read(buf, entryRange.from.toInt, entryRange.length.toInt)
          _          = markEntryUse(state, readEntry)
        } yield copied

      case Miss(addr, entryRange)                       =>
        for {
          _      <- ZIO(markCacheMiss(state))
          entry  <- makeReadEntry(state, addr)
          copied <- entry.read(buf, entryRange.from.toInt, entryRange.length.toInt)
          _       = markEntryUse(state, entry)
        } yield copied
    }
  }

  //noinspection SimplifyUnlessInspection
  private[fileio] def write(
      context: Context,
      state: State,
      offset: Long,
      data: Chunk[ByteBuffer]
  ): ZIO[DirectBufferPool with Clock, Throwable, Unit] = {
    if (data.isEmpty) ZIO.unit
    else
      for {
        lookupResults <- cacheLookup(state, offset, data.head)
        bytesWritten  <- cachedWrites(context, state, lookupResults, data.head)
        _             <- write(context, state, offset + bytesWritten, data.tail)
      } yield ()
  }

  private[fileio] def cachedWrites(
      context: Context,
      state: State,
      lookupResults: Chunk[LookupResult],
      buf: ByteBuffer,
      acc: Long = 0
  ): ZIO[DirectBufferPool with Clock, Throwable, Long] = {
    if (lookupResults.isEmpty) ZIO.succeed(acc)
    else
      for {
        written <- cachedWrite(context, state, lookupResults.head, buf)
        result  <- cachedWrites(context, state, lookupResults.tail, buf, acc + written)
      } yield result
  }

  private[fileio] def cachedWrite(
      context: Context,
      state: State,
      lookupResult: LookupResult,
      buf: ByteBuffer
  ): ZIO[DirectBufferPool with Clock, Throwable, Int] =
    lookupResult match {
      case Hit(entry @ ReadEntry(_, _, _), entryRange)  =>
        for {
          _        <- ZIO(markCacheHit(state))
          newEntry <- makeWriteEntryFromReadEntry(state, entry)
          copied   <- newEntry.write(buf, entryRange.from.toInt, entryRange.length.toInt)
          _        <- scheduleWriteOut(context, newEntry)
          _        <- DirectBufferPool.free(buf)
        } yield copied

      case Hit(entry @ WriteEntry(_, _, _), entryRange) =>
        for {
          _      <- ZIO(markCacheHit(state))
          copied <- entry.write(buf, entryRange.from.toInt, entryRange.length.toInt)
          _      <- scheduleWriteOut(context, entry)
          _      <- DirectBufferPool.free(buf)
        } yield copied

      case Miss(addr, entryRange)                       =>
        for {
          _      <- ZIO(markCacheMiss(state))
          entry  <- makeWriteEntry(state, addr)
          copied <- entry.write(buf, entryRange.from.toInt, entryRange.length.toInt)
          _      <- scheduleWriteOut(context, entry)
          _      <- DirectBufferPool.free(buf)
        } yield copied
    }

  private def markCacheHit(state: State): Unit  = state.cacheHits = state.cacheHits + 1
  private def markCacheMiss(state: State): Unit = state.cacheMisses = state.cacheMisses + 1

  private[fileio] def makeReadEntry(state: State, addr: EntryAddr): Task[ReadEntry] =
    for {
      buf  <- allocateOrFreeCacheBuf(state)
      size <- readAddr(addr, buf)
      res   = ReadEntry(addr, buf, size)
      _     = res.relevance = { state.currentRelevance += 1; state.currentRelevance }
      _     = register(state, res)
    } yield res

  private[fileio] def makeWriteEntry(state: State, addr: EntryAddr): Task[WriteEntry] =
    for {
      buf <- allocateOrFreeCacheBuf(state)
      size = entrySizeForAddr(addr)
      res  = WriteEntry(addr, buf, size)
      _    = register(state, res)
    } yield res

  private[fileio] def makeReadEntryFromWriteEntry(state: State, entry: WriteEntry): Task[ReadEntry] =
    for {
      _  <- synchronizeAndWriteOut(state, entry)
      _  <- terminate(state, entry)
      res = ReadEntry(entry.addr, entry.data, entry.dataSize)
      _   = res.relevance = { state.currentRelevance += 1; state.currentRelevance }
      _   = register(state, entry)
    } yield res

  private[fileio] def makeWriteEntryFromReadEntry(state: State, entry: ReadEntry): Task[WriteEntry] =
    for {
      _ <- terminate(state, entry)
      _  = register(state, entry)
    } yield WriteEntry(entry.addr, entry.data, entry.dataSize)

  private[fileio] def allocateOrFreeCacheBuf(state: State): Task[ByteBuffer] = {
    if (state.cacheEntries.size < MaxCacheEntries)
      Buffer.byte(CacheEntrySize)
    else
      dequeueLRU(state.cacheRelevance).flatMap {

        case entry @ ReadEntry(_, _, _)  =>
          for {
            _ <- terminate(state, entry)
            _ <- entry.data.clear
          } yield entry.data

        case entry @ WriteEntry(_, _, _) =>
          for {
            _ <- synchronizeAndWriteOut(state, entry)
            _ <- terminate(state, entry)
            _ <- entry.data.clear
          } yield entry.data
      }
  }

  @tailrec
  private[fileio] def dequeueLRU(cacheRelevance: mutable.Queue[LRUEntry]): Task[CacheEntry] = {
    if (cacheRelevance.isEmpty) ZIO.fail(new IllegalStateException("cacheRelevance.isEmpty"))
    else {
      cacheRelevance.dequeue match {

        case LRUEntry(_, cacheEntry) if cacheEntry.terminated        =>
          dequeueLRU(cacheRelevance)

        case LRUEntry(r, e @ ReadEntry(_, _, _)) if r != e.relevance =>
          dequeueLRU(cacheRelevance)

        case LRUEntry(_, entry)                                      =>
          ZIO.succeed(entry)
      }
    }
  }

  private def scheduleWriteOut(context: Context, entry: WriteEntry): ZIO[Clock, Throwable, Unit] =
    for {
      _     <- entry.writeOutFiber match {
                 case Some(fiber) => fiber.interrupt
                 case None        => ZIO.unit
               }
      self  <- context.self[Command]
      fiber <- (self ! SynchronizeAndWriteOut(entry)).delay(WriteOutDelay).fork
      _      = entry.writeOutFiber = Some(fiber)
    } yield ()

  private def markEntryUse(state: State, entry: ReadEntry): Unit = {
    state.currentRelevance += 1
    entry.relevance = state.currentRelevance
    state.cacheRelevance.enqueue(LRUEntry(state.currentRelevance, entry))
  }

  private[fileio] def synchronizeAndWriteOut(state: State, entry: WriteEntry): Task[Unit] = {

    def synchronize: Task[Unit] =
      for {
        buf <- Buffer.byte(CacheEntrySize)
        _   <- state.files(entry.addr.fileIndex).channel.read(buf, entry.addr.fileOffset)
        _   <- ZIO.foreach_(entry.freeRanges) { r =>
                 for {
                   _ <- buf.position(r.from.toInt)
                   _ <- buf.limit(r.until.toInt)
                   _ <- entry.write(buf, r.from.toInt, r.length.toInt)
                 } yield ()
               }
      } yield ()

    def writeOut: Task[Unit] =
      for {
        _ <- entry.data.position(0)
        _ <- entry.data.limit(entry.dataSize)
        _ <- state.files(entry.addr.fileIndex).channel.write(entry.data, entry.addr.fileOffset)
      } yield ()

    for {
      _ <- synchronize.unless(entry.isFull)
      _ <- writeOut
    } yield ()
  }

  private def register(state: State, entry: CacheEntry): Unit =
    state.cacheEntries.put(entry.addr, entry)

  private def terminate(state: State, entry: CacheEntry): Task[Unit] = {
    state.cacheEntries.remove(entry.addr)
    entry match {
      case e @ ReadEntry(_, _, _)  => ZIO(e.terminated = true)
      case e @ WriteEntry(_, _, _) =>
        e.writeOutFiber match {
          case Some(f) => f.interrupt.as(e.terminated = true)
          case None    => ZIO(e.terminated = true)
        }
    }
  }

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

  /** Returns amount of bytes read for given `addr` */
  def readAddr(addr: EntryAddr, buf: ByteBuffer): Task[Int] = ???
  def writeCacheEntry(entry: CacheEntry): Task[Unit]        = ???

  def entrySizeForAddr(addr: EntryAddr): Int = ???
}
