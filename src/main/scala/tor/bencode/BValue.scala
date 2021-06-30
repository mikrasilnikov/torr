package tor.bencode

import zio._
import java.nio.charset.StandardCharsets

sealed trait BValue {
  def prettyPrint: String = {
    def print(node: BValue, builder: StringBuilder, indent: Int, indentFirst: Boolean = true): StringBuilder =
      node match {
        case BInt(v)    =>
          if (indentFirst) builder.append(" " * indent)
          builder.append(s"$v")
        case BStr(v)    =>
          if (indentFirst) builder.append(" " * indent)
          if (v.size <= 256)
            builder.append(new String(v.toArray, StandardCharsets.UTF_8))
          else
            builder.append(s"(string size=${v.size})")
        case BList(vs)  =>
          if (indentFirst) builder.append(" " * indent)
          builder.append("List(\n")
          vs.foreach { v => print(v, builder, indent + 2); builder.append("\n") }
          builder.append(" " * indent)
          builder.append(")")
        case BDict(map) =>
          if (indentFirst) builder.append(" " * indent)
          builder.append("Map(\n")
          map.foreach {
            case (key, value) =>
              print(key, builder, indent + 2)
              builder.append(" => ")
              print(value, builder, indent + 2, indentFirst = false)
              builder.append("\n")
          }
          builder.append(" " * indent)
          builder.append(")")
      }

    val builder = new StringBuilder
    print(this, builder, 0)
    builder.toString
  }
}

final case class BInt(value: BigInt)             extends BValue
final case class BStr(value: Chunk[Byte])        extends BValue
final case class BList(value: List[BValue])      extends BValue
final case class BDict(value: Map[BStr, BValue]) extends BValue

object BValue {
  def string(value: String): BStr = BStr(Chunk.fromArray(value.toCharArray.map(_.toByte)))

  def list(values: BValue*): BList = BList(values.toList)

  def int(value: BigInt): BInt = BInt(value)

  def dict(pairs: (BStr, BValue)*): BDict = BDict(pairs.toMap)
}
