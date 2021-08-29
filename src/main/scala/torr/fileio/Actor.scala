package torr.fileio

import zio._
import zio.actors.Actor.Stateful
import zio.actors.{ActorRef, Context}
import zio.nio.core._
import torr.channels._
import torr.directbuffers.DirectBufferPool
import zio.clock.Clock

import java.time.temporal.TemporalAmount
import scala.annotation.tailrec

object Actor {

  sealed trait Command[+_]
  case object Fail                                                        extends Command[Unit]
  case object GetState                                                    extends Command[State]
  case class Fetch(piece: Int, pieceOffset: Int, amount: Int)             extends Command[Chunk[ByteBuffer]]
  case class Store(piece: Int, pieceOffset: Int, data: Chunk[ByteBuffer]) extends Command[Unit]

  private[fileio] case class SynchronizeAndWriteOut(writeEntry: WriteEntry) extends Command[Unit]

  sealed trait LookupResult
  case class Hit(entry: CacheEntry, entryRange: IntRange)     extends LookupResult
  case class Miss(entryAddr: EntryAddr, entryRange: IntRange) extends LookupResult

  // Multiple files in a torrent are treated like a single continuous file.
  case class OpenedFile(offset: Long, size: Long, channel: SeekableByteChannel)

  case class State(
      pieceSize: Int,
      torrentSize: Long,
      files: Vector[OpenedFile],
      cache: Cache
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
            self     <- context.self[Command]
            _        <- validateAmount(state, absolute, amount)
            _        <- write(self, state, absolute, data)
          } yield (state, ())

        case GetState                          => ZIO.succeed(state, state)
        case Fail                              => ZIO.fail(new Exception("Failed"))

        case SynchronizeAndWriteOut(writeEntry) =>
          if (state.cache.entryIsValid(writeEntry))
            makeReadEntryFromWriteEntry(state, writeEntry).as(state, ())
          else ZIO.succeed(state, ())
      }
    }
  }

  private[fileio] def read(
      state: State,
      offset: Long,
      amount: Int,
      acc: Chunk[ByteBuffer] = Chunk.empty
  ): RIO[DirectBufferPool with Clock, Chunk[ByteBuffer]] =
    if (amount <= 0) ZIO.succeed(acc)
    else
      for {
        buf           <- DirectBufferPool.allocate
        lookupResults <- cacheLookup(state, offset, math.min(amount, buf.capacity))
        bytesRead     <- cachedReads(state, lookupResults, buf)
        res           <- read(state, offset + bytesRead, amount - bytesRead, acc :+ buf)
      } yield res

  private[fileio] def cachedReads(
      state: State,
      lookupResults: Chunk[LookupResult],
      buf: ByteBuffer,
      acc: Int = 0
  ): RIO[DirectBufferPool with Clock, Int] = {
    if (lookupResults.isEmpty)
      buf.flip *> ZIO.succeed(acc)
    else
      for {
        bytesRead <- cachedRead(state, lookupResults.head, buf)
        result    <- cachedReads(state, lookupResults.tail, buf, acc + bytesRead)
      } yield result
  }

  private[fileio] def cachedRead(state: State, lookupResult: LookupResult, buf: ByteBuffer): Task[Int] = {
    lookupResult match {
      case Hit(entry @ ReadEntry(_, _, _), entryRange)     =>
        for {
          _      <- ZIO(state.cache.markHit())
          copied <- entry.read(buf, entryRange.from, entryRange.length)
          _       = state.cache.markUse(entry)
        } yield copied

      case Hit(entry @ WriteEntry(_, _, _, _), entryRange) =>
        for {
          _         <- ZIO(state.cache.markHit())
          readEntry <- makeReadEntryFromWriteEntry(state, entry)
          copied    <- readEntry.read(buf, entryRange.from, entryRange.length)
          _          = state.cache.markUse(readEntry)
        } yield copied

      case Miss(addr, entryRange)                          =>
        for {
          _      <- ZIO(state.cache.markMiss())
          entry  <- makeReadEntry(state, addr)
          copied <- entry.read(buf, entryRange.from, entryRange.length)
          _       = state.cache.markUse(entry)
        } yield copied
    }
  }

  //noinspection SimplifyUnlessInspection
  private[fileio] def write(
      self: ActorRef[Command],
      state: State,
      offset: Long,
      data: Chunk[ByteBuffer]
  ): ZIO[DirectBufferPool with Clock, Throwable, Unit] = {
    if (data.isEmpty)
      ZIO.unit
    else
      for {
        amount        <- data.head.remaining
        _             <- ZIO.fail(new IllegalArgumentException("amount == 0")).when(amount == 0)
        lookupResults <- cacheLookup(state, offset, amount)
        bytesWritten  <- cachedWrites(self, state, lookupResults, data.head)
        _             <- write(self, state, offset + bytesWritten, data.tail)
      } yield ()
  }

  private[fileio] def cachedWrites(
      self: ActorRef[Command],
      state: State,
      lookupResults: Chunk[LookupResult],
      buf: ByteBuffer,
      acc: Long = 0
  ): ZIO[DirectBufferPool with Clock, Throwable, Long] = {
    if (lookupResults.isEmpty) ZIO.succeed(acc)
    else
      for {
        written <- cachedWrite(self, state, lookupResults.head, buf)
        result  <- cachedWrites(self, state, lookupResults.tail, buf, acc + written)
      } yield result
  }

  private[fileio] def cachedWrite(
      self: ActorRef[Command],
      state: State,
      lookupResult: LookupResult,
      buf: ByteBuffer
  ): ZIO[DirectBufferPool with Clock, Throwable, Int] =
    lookupResult match {
      case Hit(entry @ ReadEntry(_, _, _), entryRange)     =>
        for {
          _        <- ZIO(state.cache.markHit())
          newEntry <- makeWriteEntryFromReadEntry(self, state, entry)
          copied   <- newEntry.write(buf, entryRange.from, entryRange.length)
          _        <- scheduleWriteOut(state, newEntry)
          _        <- DirectBufferPool.free(buf)
        } yield copied

      case Hit(entry @ WriteEntry(_, _, _, _), entryRange) =>
        for {
          _      <- ZIO(state.cache.markHit())
          copied <- entry.write(buf, entryRange.from, entryRange.length)
          _      <- scheduleWriteOut(state, entry)
          _      <- DirectBufferPool.free(buf)
        } yield copied

      case Miss(addr, entryRange)                          =>
        for {
          _      <- ZIO(state.cache.markMiss())
          entry  <- makeWriteEntry(self, state, addr)
          copied <- entry.write(buf, entryRange.from, entryRange.length)
          _      <- scheduleWriteOut(state, entry)
          _      <- DirectBufferPool.free(buf)
        } yield copied
    }

  private[fileio] def makeReadEntry(state: State, addr: EntryAddr): Task[ReadEntry] = {
    for {
      buf  <- state.cache.pullEntryToRecycle match {
                case Some(e) => recycleEntry(state, e)
                case None    => Buffer.byte(state.cache.entrySize)
              }
      size <- readAddr(state, addr, buf)
      res   = ReadEntry(addr, buf, size)
      _     = state.cache.add(res)
    } yield res
  }

  private[fileio] def makeReadEntryFromWriteEntry(state: State, entry: WriteEntry): Task[ReadEntry] =
    for {
      _  <- ZIO(state.cache.remove(entry))
      _  <- recycleEntry(state, entry)
      res = ReadEntry(entry.addr, entry.data, entry.dataSize)
      _   = state.cache.add(res)
    } yield res

  private[fileio] def makeWriteEntry(
      self: ActorRef[Command],
      state: State,
      addr: EntryAddr
  ): ZIO[Clock, Throwable, WriteEntry] = {
    for {
      buf        <- state.cache.pullEntryToRecycle match {
                      case Some(e) => recycleEntry(state, e)
                      case None    => Buffer.byte(state.cache.entrySize)
                    }
      size        = entrySizeForAddr(state, addr)
      expiration <- clock.currentDateTime.map(_.plus(state.cache.writeOutDelay))
      res         = WriteEntry(addr, buf, size, expiration)
      _           = state.cache.add(res)

      _     <- scheduleWriteOut(state, res)
      fiber <- performScheduledWriteOut(self, res).fork
      _      = res.writeOutFiber = Some(fiber)

    } yield res
  }

  private[fileio] def makeWriteEntryFromReadEntry(
      self: ActorRef[Command],
      state: State,
      entry: ReadEntry
  ): ZIO[Clock, Throwable, WriteEntry] =
    for {
      _          <- ZIO(state.cache.remove(entry))
      _          <- recycleEntry(state, entry)
      expiration <- clock.currentDateTime.map(_.plus(state.cache.writeOutDelay))
      res         = WriteEntry(entry.addr, entry.data, entry.dataSize, expiration)
      _           = state.cache.add(res)

      _     <- scheduleWriteOut(state, res)
      fiber <- performScheduledWriteOut(self, res).fork
      _      = res.writeOutFiber = Some(fiber)
    } yield res

  private[fileio] def performScheduledWriteOut(
      self: ActorRef[Command],
      entry: WriteEntry
  ): ZIO[Clock, Throwable, Unit] = {
    clock.currentDateTime.flatMap { now =>
      if (entry.expirationDateTime.compareTo(now) <= 0) {
        self ! SynchronizeAndWriteOut(entry)
      } else {
        val timeToWait = java.time.Duration.between(now, entry.expirationDateTime)
        for {
          // This does not make any sense but this sleep() is not interruptible otherwise
          _ <- ZIO.sleep(timeToWait).interruptible
          _ <- performScheduledWriteOut(self, entry)
        } yield ()
      }
    }
  }

  private[fileio] def recycleEntry(state: State, entry: CacheEntry): Task[ByteBuffer] = {

    entry match {
      case ReadEntry(_, data, _)         =>
        ZIO.succeed(data)

      case e @ WriteEntry(_, data, _, _) =>
        for {
          _ <- e.writeOutFiber match {
                 case None    => ZIO.unit
                 case Some(f) => f.interrupt
               }
          _ <- synchronizeAndWriteOut(state, e)
        } yield data
    }
  }

  private def scheduleWriteOut(
      state: State,
      entry: WriteEntry
  ): ZIO[Clock, Throwable, Unit] = {
    if (entry.isFull)
      makeReadEntryFromWriteEntry(state, entry).as()
    else
      for {
        now <- clock.currentDateTime
        _    = entry.expirationDateTime = now.plus(state.cache.writeOutDelay)
      } yield ()

  }

  /**
    * Writes out `entry` to disk.
    * If `entry` is not full (some parts of it are out of sync) then
    *
    *  - reads whole part from disk
    *  - merges data
    *  - writes result
    */
  private[fileio] def synchronizeAndWriteOut(state: State, entry: WriteEntry): Task[Unit] = {

    def synchronize: Task[Unit] =
      for {
        buf       <- Buffer.byte(state.cache.entrySize)
        fileOffset = entry.addr.entryIndex * state.cache.entrySize
        _         <- state.files(entry.addr.fileIndex).channel.read(buf, fileOffset)
        _         <- ZIO.foreach_(entry.freeRanges) { r =>
                       for {
                         _ <- buf.limit(r.until)
                         _ <- buf.position(r.from)
                         _ <- entry.write(buf, r.from, r.length)
                       } yield ()
                     }
      } yield ()

    def writeOut: Task[Unit] =
      for {
        _         <- entry.data.position(0)
        _         <- entry.data.limit(entry.dataSize)
        fileOffset = entry.addr.entryIndex * state.cache.entrySize
        _         <- state.files(entry.addr.fileIndex).channel.write(entry.data, fileOffset)
      } yield ()

    for {
      _ <- synchronize.unless(entry.isFull)
      _ <- writeOut
    } yield ()
  }

  /**
    *                 ==Example1==
    * {{{
    *            cached
    * ... |-----------------| ... <- file
    *       |------------|        <- requested range (not aligned)
    *             ^
    *            hit
    * }}}
    *                == Example2 ==
    * {{{
    *            cached          not cached
    * ... |-----------------|-----------------| ... <- file
    *                  |----+--------|              <- requested range (not aligned)
    *                     ^      ^
    *                    hit    miss
    * }}}
    *                == Example3 ==
    * {{{
    *                      not cached    cached
    * ... |-----------------|-----| eof                    <- file1
    *                             |-----------------| ...  <- file2
    *                        |----+--------|               <- requested range
    *                          ^      ^
    *                         miss   hit
    * }}}
    */
  private[fileio] def cacheLookup(
      state: State,
      torrentOffset: Long,
      amount: Int,
      acc: Chunk[LookupResult] = Chunk.empty
  ): Task[Chunk[LookupResult]] = {
    if (amount <= 0) ZIO.succeed(acc)
    else {
      val (entryAddr, entryOffset) = locateEntryPart(state, torrentOffset)
      val entrySize                = entrySizeForAddr(state, entryAddr)
      val amountFromEntry          = math.min(amount, entrySize - entryOffset)

      state.cache.addrToEntry.get(entryAddr) match {
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
    val entryIndex              = fileOffset / state.cache.entrySize
    val entryOffset             = fileOffset % state.cache.entrySize

    // entryOffset < entrySize so it's safe to convert it to integer
    (EntryAddr(fileIndex, entryIndex), entryOffset.toInt)
  }

  private[fileio] def absoluteOffset(state: State, piece: Int, pieceOffset: Int): Long =
    piece.toLong * state.pieceSize + pieceOffset

  private[fileio] def validateAmount(state: State, absoluteOffset: Long, amount: Long): Task[Unit] =
    ZIO.fail(new IllegalArgumentException("Access beyond torrent size")).when(
      absoluteOffset + amount > state.torrentSize
    )

  /**
    * Translates global torrent offset into file index & file offset.
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
  def readAddr(state: State, addr: EntryAddr, buf: ByteBuffer): Task[Int] = {
    val position = addr.entryIndex * state.cache.entrySize
    for {
      _    <- buf.clear
      size <- state.files(addr.fileIndex).channel.read(buf, position)
    } yield size
  }

  def entrySizeForAddr(state: State, addr: EntryAddr): Int = {
    val fileOffset    = addr.entryIndex * state.cache.entrySize
    val fileRemaining = state.files(addr.fileIndex).size - fileOffset
    math.min(state.cache.entrySize.toLong, fileRemaining).toInt
  }
}
