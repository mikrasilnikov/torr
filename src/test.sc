import zio.Chunk
import zio.Chunk.BitChunk

import scala.collection.mutable
import scala.collection.immutable

// можно ли использовать BitChunk для отображения бит?

val bitSet1 = immutable.BitSet.fromBitMask(Array.ofDim[Long](1))
bitSet1.size

val bitSet2 = mutable.BitSet.fromBitMask(Array.ofDim[Long](1))
bitSet2.size

val array = Array.ofDim[Byte](4)
array(0) = (0x01 << 7).toByte

val chunk = Chunk.fromArray(array)
chunk.asBits.toBinaryString




