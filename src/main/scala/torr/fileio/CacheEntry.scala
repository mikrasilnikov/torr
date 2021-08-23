package torr.fileio

import zio._
import zio.nio.core.ByteBuffer
import scala.annotation.tailrec
import scala.collection.mutable
import torr.fileio

sealed trait CacheEntry {
  def addr: EntryAddr
  def data: ByteBuffer
  def dataSize: Int
}

case class ReadEntry(addr: EntryAddr, data: ByteBuffer, dataSize: Int) extends CacheEntry {

  def read(buf: ByteBuffer, entryOffset: Int, amount: Int): Task[Int] =
    for {
      _ <- data.limit(entryOffset + amount)
      _ <- data.position(entryOffset)
      _ <- buf.putByteBuffer(data)
    } yield amount
}

case class WriteEntry(addr: EntryAddr, data: ByteBuffer, dataSize: Int) extends CacheEntry {

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
        case h :: t                   => loop(t, IntRange(last, h.from) :: free, h.until)
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

case class EntryAddr(fileIndex: Int, entryIndex: Long)

case class LongRange(from: Long, until: Long) {
  val length: Long = until - from
}

case class IntRange(from: Int, until: Int) {
  val length: Int = until - from
}

object CacheEntry {
  implicit val entryAddrOrdering: Ordering[EntryAddr] = new Ordering[EntryAddr] {
    override def compare(x: EntryAddr, y: EntryAddr): Int =
      x.fileIndex compare y.fileIndex match {
        case 0 => x.entryIndex compare y.entryIndex
        case x => x
      }
  }
}
