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

  case class CacheState(
      entrySize: Int = 128 * 1024,
      cacheEntriesNum: Int = 100,
      writeOutDelay: Duration = 30.seconds,
      cacheEntries: mutable.HashMap[EntryAddr, CacheEntry] = new mutable.HashMap[EntryAddr, CacheEntry],
      relevances: mutable.Queue[LRUEntry] = new mutable.Queue[LRUEntry],
      var currentRelevance: Long = 0,
      var cacheHits: Long = 0,
      var cacheMisses: Long = 0
  )

  case class State(
      pieceSize: Int,
      torrentSize: Long,
      files: Vector[OpenedFile],
      cacheState: CacheState
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
        lookupResults <- cacheLookup(state, offset, buf.capacity)
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
          _      <- ZIO(markCacheHit(state.cacheState))
          copied <- entry.read(buf, entryRange.from, entryRange.length)
          _       = markEntryUse(state.cacheState, entry)
        } yield copied

      case Hit(entry @ WriteEntry(_, _, _), entryRange) =>
        for {
          _         <- ZIO(markCacheHit(state.cacheState))
          readEntry <- makeReadEntryFromWriteEntry(state, entry)
          copied    <- readEntry.read(buf, entryRange.from, entryRange.length)
          _          = markEntryUse(state.cacheState, readEntry)
        } yield copied

      case Miss(addr, entryRange)                       =>
        for {
          _      <- ZIO(markCacheMiss(state.cacheState))
          entry  <- makeReadEntry(state, addr)
          copied <- entry.read(buf, entryRange.from, entryRange.length)
          _       = markEntryUse(state.cacheState, entry)
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
        amount        <- data.head.remaining
        lookupResults <- cacheLookup(state, offset, amount)
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
          _        <- ZIO(markCacheHit(state.cacheState))
          newEntry <- makeWriteEntryFromReadEntry(state.cacheState, entry)
          copied   <- newEntry.write(buf, entryRange.from, entryRange.length)
          _        <- scheduleWriteOut(context, state.cacheState, newEntry)
          _        <- DirectBufferPool.free(buf)
        } yield copied

      case Hit(entry @ WriteEntry(_, _, _), entryRange) =>
        for {
          _      <- ZIO(markCacheHit(state.cacheState))
          copied <- entry.write(buf, entryRange.from, entryRange.length)
          _      <- scheduleWriteOut(context, state.cacheState, entry)
          _      <- DirectBufferPool.free(buf)
        } yield copied

      case Miss(addr, entryRange)                       =>
        for {
          _      <- ZIO(markCacheMiss(state.cacheState))
          entry  <- makeWriteEntry(state, addr)
          copied <- entry.write(buf, entryRange.from, entryRange.length)
          _      <- scheduleWriteOut(context, state.cacheState, entry)
          _      <- DirectBufferPool.free(buf)
        } yield copied
    }

  private def markCacheHit(cacheState: CacheState): Unit  = cacheState.cacheHits = cacheState.cacheHits + 1
  private def markCacheMiss(cacheState: CacheState): Unit = cacheState.cacheMisses = cacheState.cacheMisses + 1

  private[fileio] def makeReadEntry(state: State, addr: EntryAddr): Task[ReadEntry] =
    for {
      buf  <- allocateOrFreeCacheBuf(state)
      size <- readAddr(addr, buf)
      res   = ReadEntry(addr, buf, size)
      _     = res.relevance = { state.cacheState.currentRelevance += 1; state.cacheState.currentRelevance }
      _     = register(state.cacheState, res)
    } yield res

  private[fileio] def makeWriteEntry(state: State, addr: EntryAddr): Task[WriteEntry] =
    for {
      buf <- allocateOrFreeCacheBuf(state)
      size = entrySizeForAddr(addr)
      res  = WriteEntry(addr, buf, size)
      _    = register(state.cacheState, res)
    } yield res

  private[fileio] def makeReadEntryFromWriteEntry(state: State, entry: WriteEntry): Task[ReadEntry] =
    for {
      _  <- synchronizeAndWriteOut(state, entry)
      _  <- terminate(state.cacheState, entry)
      res = ReadEntry(entry.addr, entry.data, entry.dataSize)
      _   = res.relevance = { state.cacheState.currentRelevance += 1; state.cacheState.currentRelevance }
      _   = register(state.cacheState, entry)
    } yield res

  private[fileio] def makeWriteEntryFromReadEntry(cacheState: CacheState, entry: ReadEntry): Task[WriteEntry] =
    for {
      _ <- terminate(cacheState, entry)
      _  = register(cacheState, entry)
    } yield WriteEntry(entry.addr, entry.data, entry.dataSize)

  private[fileio] def allocateOrFreeCacheBuf(state: State): Task[ByteBuffer] = {
    if (state.cacheState.cacheEntries.size < state.cacheState.cacheEntriesNum)
      Buffer.byte(state.cacheState.entrySize)
    else
      dequeueLRU(state.cacheState.relevances).flatMap {

        case entry @ ReadEntry(_, _, _)  =>
          for {
            _ <- terminate(state.cacheState, entry)
            _ <- entry.data.clear
          } yield entry.data

        case entry @ WriteEntry(_, _, _) =>
          for {
            _ <- synchronizeAndWriteOut(state, entry)
            _ <- terminate(state.cacheState, entry)
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

  private def scheduleWriteOut(
      context: Context,
      cacheState: CacheState,
      entry: WriteEntry
  ): ZIO[Clock, Throwable, Unit] =
    for {
      _     <- entry.writeOutFiber match {
                 case Some(fiber) => fiber.interrupt
                 case None        => ZIO.unit
               }
      self  <- context.self[Command]
      fiber <- (self ! SynchronizeAndWriteOut(entry)).delay(cacheState.writeOutDelay).fork
      _      = entry.writeOutFiber = Some(fiber)
    } yield ()

  private def markEntryUse(cacheState: CacheState, entry: ReadEntry): Unit = {
    cacheState.currentRelevance += 1
    entry.relevance = cacheState.currentRelevance
    cacheState.relevances.enqueue(LRUEntry(cacheState.currentRelevance, entry))
  }

  private[fileio] def synchronizeAndWriteOut(state: State, entry: WriteEntry): Task[Unit] = {

    def synchronize: Task[Unit] =
      for {
        buf       <- Buffer.byte(state.cacheState.entrySize)
        fileOffset = entry.addr.entryIndex * state.cacheState.entrySize
        _         <- state.files(entry.addr.fileIndex).channel.read(buf, fileOffset)
        _         <- ZIO.foreach_(entry.freeRanges) { r =>
                       for {
                         _ <- buf.position(r.from)
                         _ <- buf.limit(r.until)
                         _ <- entry.write(buf, r.from, r.length)
                       } yield ()
                     }
      } yield ()

    def writeOut: Task[Unit] =
      for {
        _         <- entry.data.position(0)
        _         <- entry.data.limit(entry.dataSize)
        fileOffset = entry.addr.entryIndex * state.cacheState.entrySize
        _         <- state.files(entry.addr.fileIndex).channel.write(entry.data, fileOffset)
      } yield ()

    for {
      _ <- synchronize.unless(entry.isFull)
      _ <- writeOut
    } yield ()
  }

  private def register(cacheState: CacheState, entry: CacheEntry): Unit =
    cacheState.cacheEntries.put(entry.addr, entry)

  private def terminate(cacheState: CacheState, entry: CacheEntry): Task[Unit] = {
    cacheState.cacheEntries.remove(entry.addr)
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
      torrentOffset: Long,
      amount: Int,
      acc: Chunk[LookupResult] = Chunk.empty
  ): Task[Chunk[LookupResult]] = {
    if (amount <= 0) ZIO.succeed(acc)
    else {
      val (entryAddr, entryOffset) = locateEntryPart(state, torrentOffset)
      val amountFromEntry          = math.min(amount, state.cacheState.entrySize - entryOffset)

      state.cacheState.cacheEntries.get(entryAddr) match {
        case None    =>
          val miss = Miss(entryAddr, IntRange(entryOffset, entryOffset + amountFromEntry))
          cacheLookup(
            state,
            torrentOffset + amountFromEntry,
            amount - amountFromEntry,
            acc :+ miss
          )

        case Some(e) =>
          val hit = Hit(e, IntRange(entryOffset, entryOffset + amountFromEntry))
          cacheLookup(
            state,
            torrentOffset + amountFromEntry,
            amount - amountFromEntry,
            acc :+ hit
          )
      }
    }
  }

  /** absolute torrent offset -> (entry address, entry offset) */
  private[fileio] def locateEntryPart(state: State, torrentOffset: Long): (EntryAddr, Int) = {

    val (fileIndex, fileOffset) = fileIndexOffset(state, torrentOffset)
    val entryIndex              = fileOffset / state.cacheState.entrySize
    val entryOffset             = fileOffset % state.cacheState.entrySize

    // entryOffset < entrySize so it's safe to convert it to integer
    (EntryAddr(fileIndex, entryIndex), entryOffset.toInt)
  }

  private[fileio] def absoluteOffset(state: State, piece: Int, pieceOffset: Int): Long             = ???
  private[fileio] def validateAmount(state: State, absoluteOffset: Long, amount: Long): Task[Unit] = ???

  /**
    * Translates piece index & piece offset into file index & file offset.
    */
  private[fileio] def fileIndexOffset(state: State, torrentOffset: Long): (Int, Long) = {

    val e = torrentOffset

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
            s"Could not find file by offset. Files: ${state.files.mkString(",")}; offset=$torrentOffset"
          )
      }
    }

    val index  = loop(0, state.files.length)
    val offset = e - state.files(index).offset

    (index, offset)
  }

  /** Returns amount of bytes read for given `addr` */
  def readAddr(addr: EntryAddr, buf: ByteBuffer): Task[Int] = ???

  def entrySizeForAddr(addr: EntryAddr): Int = ???
}
