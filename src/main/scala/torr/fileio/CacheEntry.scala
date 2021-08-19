package torr.fileio

import torr.fileio
import zio._
import zio.nio.core.ByteBuffer

import scala.annotation.tailrec
import scala.collection.mutable

sealed trait CacheEntry {
  def addr: EntryAddr
  def data: ByteBuffer
  def dataSize: Int
  def terminated: Boolean
}

case class ReadEntry(addr: EntryAddr, data: ByteBuffer, dataSize: Int) extends CacheEntry {
  var relevance: Long     = 0
  var terminated: Boolean = false

  def read(buf: ByteBuffer, entryOffset: Int, amount: Int): Task[Int] =
    for {
      _ <- data.limit(entryOffset + amount)
      _ <- data.position(entryOffset)
      _ <- buf.putByteBuffer(data)
    } yield amount
}

case class WriteEntry(addr: EntryAddr, data: ByteBuffer, dataSize: Int) extends CacheEntry {

  var terminated: Boolean = false

  private val usedRanges: mutable.TreeSet[IntRange] = new mutable.TreeSet[IntRange]()(
    new Ordering[IntRange] {
      override def compare(x: IntRange, y: IntRange): Int = x.from.compareTo(y.from)
    }
  )

  def isFull: Boolean = freeRanges.isEmpty

  def freeRanges: List[IntRange] = {

    @tailrec
    def loop(used: List[IntRange], free: List[IntRange] = Nil, last: Int = 0): List[IntRange] =
      used match {
        case Nil                      =>
          if (last != dataSize) IntRange(last, dataSize) :: free
          else free

        case h :: t if h.from <= last => loop(t, free, math.max(last, h.until))

        case h :: t => loop(t, IntRange(last, h.from) :: free, h.until)
      }

    if (usedRanges.isEmpty) List(IntRange(0, dataSize))
    else loop(usedRanges.toList)
  }

  def write(buf: ByteBuffer, entryOffset: Int, amount: Int): Task[Int] =
    for {
      oldPos <- buf.position
      oldLim <- buf.limit

      _ <- buf.limit(oldPos + amount)
      _ <- data.limit(entryOffset + amount)
      _ <- data.position(entryOffset)
      _ <- data.putByteBuffer(buf)

      _ = usedRanges += IntRange(entryOffset, entryOffset + amount)

      _ <- buf.position(oldPos)
      _ <- buf.limit(oldLim)
    } yield amount

  private[fileio] var writeOutFiber: Option[Fiber[Throwable, Unit]] = None
}

case class EntryAddr(fileIndex: Int, entryIndex: Long) {
  val fileOffset = entryIndex * fileio.Actor.CacheEntrySize
}

case class LongRange(from: Long, until: Long) {
  val length: Long = until - from
}

case class IntRange(from: Int, until: Int) {
  val length: Int = until - from
}
