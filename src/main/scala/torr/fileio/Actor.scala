package torr.fileio

import zio._
import zio.actors.Actor.Stateful
import zio.actors.{ActorRef, Context}
import zio.nio.core._
import torr.channels._
import torr.directbuffers.DirectBufferPool
import zio.clock.Clock
import java.time.{Duration, OffsetDateTime}
import scala.annotation.tailrec

object Actor {

  sealed trait Command[+_]
  case object Fail                                                        extends Command[Unit]
  case object GetState                                                    extends Command[State]
  case class Fetch(piece: Int, pieceOffset: Int, amount: Int)             extends Command[Chunk[ByteBuffer]]
  case class Store(piece: Int, pieceOffset: Int, data: Chunk[ByteBuffer]) extends Command[Unit]
  case object Flush                                                       extends Command[Unit]

  private[fileio] case class CheckWriteOutConditions(writeEntry: WriteEntry) extends Command[Unit]

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
          val absolute = absoluteOffset(state, piece, pieceOffset)
          for {
            _      <- validateAmount(state, absolute, amount)
            result <- read(state, absolute, amount)
          } yield (state, result)

        case Store(piece, pieceOffset, data)   =>
          val absolute = absoluteOffset(state, piece, pieceOffset)
          for {
            amount <- ZIO.foldLeft(data)(0L) { case (acc, buf) => buf.remaining.map(acc + _) }
            //_      <- ZIO(println(s"amount0 == $amount, piece = $piece, pieceOffset = $pieceOffset")).when(amount != 32768)
            self   <- context.self[Command]
            _      <- validateAmount(state, absolute, amount)
            _      <- write(self, state, absolute, data)
          } yield (state, ())

        case GetState                          => ZIO.succeed(state, state)
        case Fail                              => ZIO.fail(new Exception("Failed"))

        case CheckWriteOutConditions(writeEntry) =>
          for {
            self <- context.self[Command]
            _    <- checkWriteOutConditions(self, state, writeEntry)
          } yield (state, ())
      }
    }
  }

  private[fileio] def read(
      state: State,
      offset: Long,
      amount: Int,
      acc: Chunk[ByteBuffer] = Chunk.empty,
      curBuf: Option[ByteBuffer] = None
  ): RIO[DirectBufferPool with Clock, Chunk[ByteBuffer]] =
    for {
      buf       <- curBuf match {
                     case Some(b) => ZIO.succeed(b)
                     case None    => DirectBufferPool.allocate
                   }

      bufRem    <- buf.remaining
      lookupRes  = cacheLookup(state, offset, math.min(amount, bufRem))
      bytesRead <- cachedRead(state, lookupRes, buf)

      newBufRem = bufRem - bytesRead
      newOffset = offset + bytesRead
      newAmount = amount - bytesRead

      res <- (newBufRem, newAmount) match {
               case (_, 0) => buf.flip *> ZIO.succeed(acc :+ buf)
               case (0, _) => buf.flip *> read(state, newOffset, newAmount, acc :+ buf, None)
               case (_, _) => read(state, newOffset, newAmount, acc, Some(buf))
             }
    } yield res

  private[fileio] def cachedRead(state: State, lookupResult: LookupResult, buf: ByteBuffer): Task[Int] = {
    lookupResult match {
      case Hit(entry @ ReadEntry(_, _, _), entryRange)     =>
        state.cache.markHit()
        for {
          copied <- entry.read(buf, entryRange.from, entryRange.length)
          _       = state.cache.markUse(entry)
        } yield copied

      case Hit(entry @ WriteEntry(_, _, _, _), entryRange) =>
        state.cache.markHit()
        for {
          readEntry <- makeReadEntryFromWriteEntry(state, entry)
          copied    <- readEntry.read(buf, entryRange.from, entryRange.length)
          _          = state.cache.markUse(readEntry)
        } yield copied

      case Miss(addr, entryRange)                          =>
        state.cache.markMiss()
        for {
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
    if (data.isEmpty) ZIO.unit
    else
      for {
        bufRem       <- data.head.remaining
        lookupRes     = cacheLookup(state, offset, bufRem)
        bytesWritten <- cachedWrite(self, state, lookupRes, data.head)

        newBufRem = bufRem - bytesWritten
        newOffset = offset + bytesWritten

        _ <- newBufRem match {
               case 0 => DirectBufferPool.free(data.head) *> write(self, state, newOffset, data.tail)
               case _ => write(self, state, newOffset, data)
             }

      } yield ()
  }

  private[fileio] def cachedWrite(
      self: ActorRef[Command],
      state: State,
      lookupResult: LookupResult,
      buf: ByteBuffer
  ): ZIO[DirectBufferPool with Clock, Throwable, Int] = {
    lookupResult match {
      case Hit(entry @ ReadEntry(_, _, _), entryRange)     =>
        state.cache.markHit()
        for {
          newEntry <- makeWriteEntryFromReadEntry(self, state, entry)
          copied   <- newEntry.write(buf, entryRange.from, entryRange.length)
          _        <- updateExpirationTime(state, newEntry)
        } yield copied

      case Hit(entry @ WriteEntry(_, _, _, _), entryRange) =>
        state.cache.markHit()
        for {
          copied <- entry.write(buf, entryRange.from, entryRange.length)
          _      <- updateExpirationTime(state, entry)
        } yield copied

      case Miss(addr, entryRange)                          =>
        state.cache.markMiss()
        for {
          entry  <- makeWriteEntry(self, state, addr)
          copied <- entry.write(buf, entryRange.from, entryRange.length)
          _      <- updateExpirationTime(state, entry)
        } yield copied
    }
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

  private[fileio] def makeReadEntryFromWriteEntry(state: State, entry: WriteEntry): Task[ReadEntry] = {
    for {
      _  <- ZIO(state.cache.remove(entry))
      _  <- recycleEntry(state, entry)
      res = ReadEntry(entry.addr, entry.data, entry.dataSize)
      _   = state.cache.add(res)
    } yield res
  }

  private[fileio] def makeWriteEntry(
      self: ActorRef[Command],
      state: State,
      addr: EntryAddr
  ): ZIO[Clock, Throwable, WriteEntry] = {
    for {
      buf <- state.cache.pullEntryToRecycle match {
               case Some(e) => recycleEntry(state, e)
               case None    => Buffer.byte(state.cache.entrySize)
             }
      size = entrySizeForAddr(state, addr)
      res <- makeWriteEntryCore(self, state.cache, addr, buf, size)
    } yield res
  }

  private[fileio] def makeWriteEntryFromReadEntry(
      self: ActorRef[Command],
      state: State,
      entry: ReadEntry
  ): ZIO[Clock, Throwable, WriteEntry] = {
    for {
      _   <- ZIO(state.cache.remove(entry))
      _   <- recycleEntry(state, entry)
      res <- makeWriteEntryCore(self, state.cache, entry.addr, entry.data, entry.dataSize)
    } yield res
  }

  private[fileio] def makeWriteEntryCore(
      self: ActorRef[Command],
      cache: Cache,
      addr: EntryAddr,
      data: ByteBuffer,
      size: Int
  ): ZIO[Clock, Throwable, WriteEntry] = {
    for {
      expiration <- clock.currentDateTime.map(_.plus(cache.writeOutDelay))
      res         = WriteEntry(addr, data, size, expiration)
      fiber      <- triggerWriteOutCheck(self, res, expiration).fork
      _           = res.writeOutFiber = Some(fiber)
      _           = cache.add(res)
    } yield res
  }

  private[fileio] def triggerWriteOutCheck(
      self: ActorRef[Command],
      entry: WriteEntry,
      when: OffsetDateTime
  ): ZIO[Clock, Throwable, Unit] = {
    clock.currentDateTime.flatMap { now =>
      val timeToWait = Duration.between(now, when)
      for {
        // Doesn't make any sense but this sleep() is not interruptible otherwise.
        _ <- ZIO.sleep(timeToWait).interruptible
        _ <- self ! CheckWriteOutConditions(entry)
      } yield ()
    }
  }

  //noinspection SimplifyWhenInspection
  private def checkWriteOutConditions(
      self: ActorRef[Command],
      state: State,
      entry: WriteEntry
  ): ZIO[Clock, Throwable, Unit] = {
    if (state.cache.entryIsValid(entry)) {
      clock.currentDateTime.flatMap { now =>
        if (entry.expirationDateTime.compareTo(now) <= 0) {
          makeReadEntryFromWriteEntry(state, entry).as()
        } else {
          for {
            _     <- entry.writeOutFiber.get.join
            fiber <- triggerWriteOutCheck(self, entry, entry.expirationDateTime).fork
            _      = entry.writeOutFiber = Some(fiber)
          } yield ()
        }
      }
    } else
      ZIO.unit
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

  private def updateExpirationTime(
      state: State,
      entry: WriteEntry
  ): ZIO[Clock, Throwable, Unit] = {
    if (entry.isFull) {
      makeReadEntryFromWriteEntry(state, entry).as()
    } else
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
    *                     ^
    *                    hit
    * }}}
    *                == Example3 ==
    * {{{
    *                      not cached    cached
    * ... |-----------------|-----| eof                    <- file1
    *                             |-----------------| ...  <- file2
    *                        |----+--------|               <- requested range
    *                          ^
    *                         miss
    * }}}
    */
  private[fileio] def cacheLookup(
      state: State,
      torrentOffset: Long,
      amount: Int
  ): LookupResult = {
    val (entryAddr, entryOffset) = locateEntryPart(state, torrentOffset)
    val entrySize                = entrySizeForAddr(state, entryAddr)
    val amountFromEntry          = math.min(amount, entrySize - entryOffset)

    state.cache.addrToEntry.get(entryAddr) match {
      case None    => Miss(entryAddr, IntRange(entryOffset, entryOffset + amountFromEntry))
      case Some(e) => Hit(e, IntRange(entryOffset, entryOffset + amountFromEntry))
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
