package tor.bencode

import zio.Chunk

sealed trait BValue                              extends Any
final case class BInt(value: BigInt)             extends AnyVal with BValue
final case class BStr(value: Chunk[Byte])        extends AnyVal with BValue
final case class BList(value: List[BValue])      extends AnyVal with BValue
final case class BDict(value: Map[BStr, BValue]) extends AnyVal with BValue

object BValue {
  def string(value: String): BStr = BStr(Chunk.fromArray(value.toCharArray.map(_.toByte)))

  def list(values: BValue*): BList = BList(values.toList)

  def int(value: BigInt): BInt = BInt(value)

  def dict(pairs: (BStr, BValue)*): BDict = BDict(pairs.toMap)
}
