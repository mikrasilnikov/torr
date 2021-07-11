package torr.bencode

import zio._
import java.nio.charset.StandardCharsets
import scala.language.implicitConversions
import torr.misc.Traverse

sealed trait BValue {

  def prettyPrint: String = {

    def print(node: BValue, builder: StringBuilder, indent: Int, indentFirst: Boolean = true): StringBuilder =
      node match {
        case BInt(v)   =>
          if (indentFirst) builder.append(" " * indent)
          builder.append(s"$v")
        case BStr(v)   =>
          if (indentFirst) builder.append(" " * indent)
          if (v.size <= 256)
            builder.append(new String(v.toArray, StandardCharsets.UTF_8))
          else
            builder.append(s"(string size=${v.size})")
        case BList(vs) =>
          if (indentFirst) builder.append(" " * indent)
          builder.append("List(\n")
          vs.foreach { v => print(v, builder, indent + 2); builder.append("\n") }
          builder.append(" " * indent)
          builder.append(")")
        case BMap(map) =>
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

  def /(key: String): Option[BValue] = {
    this match {
      case BMap(map) => map.get(key)
      case _         => None
    }
  }

  def asInt: Option[Long] = {
    this match {
      case BInt(v) => Some(v)
      case _       => None
    }
  }

  def asString: Option[String] = {
    this match {
      case BStr(v) => Some(new String(v.toArray, StandardCharsets.UTF_8))
      case _       => None
    }
  }

  def asChunk: Option[Chunk[Byte]] = {
    this match {
      case BStr(v) => Some(v)
      case _       => None
    }
  }

  def asDictionary: Option[Map[BStr, BValue]] = {
    this match {
      case BMap(v) => Some(v)
      case _       => None
    }
  }

  def asList: Option[List[BValue]] = {
    this match {
      case BList(v) => Some(v)
      case _        => None
    }
  }

  def asStringList: Option[List[String]] = {
    this match {
      case BList(v) => Traverse.sequence(v.map(_.asString))
      case _        => None
    }
  }
}

final case class BInt(value: Long)              extends BValue
final case class BStr(value: Chunk[Byte])       extends BValue
final case class BList(value: List[BValue])     extends BValue
final case class BMap(value: Map[BStr, BValue]) extends BValue

object BValue {
  def string(value: String): BStr = BStr(Chunk.fromArray(value.getBytes(StandardCharsets.UTF_8)))

  def list(values: BValue*): BList = BList(values.toList)

  def int(value: Long): BInt = BInt(value)

  def dict(pairs: (BStr, BValue)*): BMap = BMap(pairs.toMap)

  implicit def stringToBStr(s: String): BStr = string(s)

  implicit class OptionBValueOps(opt: Option[BValue]) {
    def /(key: String): Option[BValue]     = opt.flatMap(bv => bv / key)
    def asChunk: Option[Chunk[Byte]]       = opt.flatMap(bv => bv.asChunk)
    def asString: Option[String]           = opt.flatMap(bv => bv.asString)
    def asLong: Option[Long]               = opt.flatMap(bv => bv.asInt)
    def asList: Option[List[BValue]]       = opt.flatMap(bv => bv.asList)
    def asStringList: Option[List[String]] = opt.flatMap(bv => bv.asStringList)
  }

}
