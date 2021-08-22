package torr.fileio

import zio.duration.{Duration, durationInt}
import scala.collection.mutable

case class Cache(
    entrySize: Int = 128 * 1024,
    cacheEntriesNum: Int = 100,
    writeOutDelay: Duration = 30.seconds,
    addrToEntry: mutable.HashMap[EntryAddr, CacheEntry] = new mutable.HashMap[EntryAddr, CacheEntry],
    addrToRelevance: mutable.HashMap[EntryAddr, Long] = new mutable.HashMap[EntryAddr, Long],
    relevanceToAddr: mutable.TreeMap[Long, EntryAddr] = new mutable.TreeMap[Long, EntryAddr],
    var currentRelevance: Long = 0,
    var cacheHits: Long = 0,
    var cacheMisses: Long = 0
) {

  def entryIsValid(entry: CacheEntry): Boolean = {
    addrToEntry.get(entry.addr) match {
      case Some(e) => entry eq e
      case None    => false
    }
  }

  def pullEntryToRecycle: Option[CacheEntry] = {
    if (addrToEntry.size < cacheEntriesNum) None
    else {
      val (relevance, addr) = relevanceToAddr.head
      val entry             = addrToEntry(addr)

      addrToEntry.remove(addr)
      addrToRelevance.remove(addr)
      relevanceToAddr.remove(relevance)

      Some(entry)
    }
  }

  def add(entry: CacheEntry): Unit = {
    val relevance = currentRelevance; currentRelevance += 1

    addrToEntry.put(entry.addr, entry)
    addrToRelevance.put(entry.addr, relevance)
    relevanceToAddr.put(relevance, entry.addr)
  }

  def remove(entry: CacheEntry): Unit = {
    val addr      = entry.addr
    val relevance = addrToRelevance(addr)

    addrToEntry.remove(addr)
    addrToRelevance.remove(addr)
    relevanceToAddr.remove(relevance)
  }

  def markUse(entry: CacheEntry): Unit = {
    val addr         = entry.addr
    val oldRelevance = addrToRelevance(addr)

    val newRelevance = currentRelevance; currentRelevance += 1

    addrToRelevance.update(addr, newRelevance)
    relevanceToAddr.remove(oldRelevance)
    relevanceToAddr.put(newRelevance, addr)
  }

  def markHit(): Unit  = cacheHits += 1
  def markMiss(): Unit = cacheMisses += 1
}
