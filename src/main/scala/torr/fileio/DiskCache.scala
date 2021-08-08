package torr.fileio

import zio.nio.core._
import scala.collection.mutable

object DiskCache {

  val CacheEntrySize: Int = 128 * 1024

  case class CacheEntryLocation(fileIndex: Int, fileOffset: Int)
  case class CacheEntry(buf: ByteBuffer, dirty: Boolean, var relevance: Long)
  case class LRU(relevance: Long, entry: CacheEntry)

  private val entriesByLocation = mutable.Map[CacheEntryLocation, CacheEntry]()

  private val lruOrderingByRelevance = new Ordering[LRU] {
    def compare(x: LRU, y: LRU): Int = x.relevance.compareTo(y.relevance)
  }

  val lruEntries = mutable.PriorityQueue[LRU]()(lruOrderingByRelevance)

}
