package torr.peerwire

import zio._
import java.nio._
import scala.collection.mutable

// BitSet wrapper that allows serialization according to BitTorrent protocol specification (BitField message).
case class TorrBitSet private (set: mutable.BitSet, length: Int) {

  /*
    From protocol spec (https://wiki.theory.org/BitTorrentSpecification#bitfield:_.3Clen.3D0001.2BX.3E.3Cid.3D5.3E.3Cbitfield.3E)
    The high bit in the first byte corresponds to piece index 0. Bits that are cleared indicated a missing piece,
    and set bits indicate a valid and available piece. Spare bits at the end are set to zero.
   */
  def toBytes: Chunk[Byte] = {
    val data = set.toBitMask

    var i = 0
    while (i < data.length) {
      data(i) = java.lang.Long.reverse(data(i))
      i += 1
    }

    val buf = ByteBuffer.allocate(data.length * 8)
    buf.asLongBuffer().put(data)
    Chunk.fromByteBuffer(buf).slice(0, math.ceil(length.toDouble / 8).toInt)
  }
}

object TorrBitSet {
  def make(length: Int): TorrBitSet = {
    val data = Array.ofDim[Long](math.ceil(length.toDouble / 64).toInt)
    TorrBitSet(mutable.BitSet.fromBitMaskNoCopy(data), length)
  }
}
