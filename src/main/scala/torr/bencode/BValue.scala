package torr.bencode

import torr.channels.InMemoryWritableChannel
import zio._

import java.nio.charset.StandardCharsets
import scala.language.implicitConversions
import torr.misc.Traverse
import zio.nio.core.Buffer

sealed trait BValue {

  def prettyPrint(maxStringSize: Int = 256): String = {

    def print(node: BValue, builder: StringBuilder, indent: Int, indentFirst: Boolean = true): StringBuilder =
      node match {
        case BInt(v)   =>
          if (indentFirst) builder.append(" " * indent)
          builder.append(s"$v")
        case BStr(v)   =>
          if (indentFirst) builder.append(" " * indent)
          if (v.size <= maxStringSize)
            builder.append(printBinaryString(v.toArray))
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

  def getSHA1: Task[Chunk[Byte]] = {
    for {
      channel <- InMemoryWritableChannel.make
      _       <- BEncode.write(this, channel)
      data    <- channel.data.get.map(_.toArray)
      md       = java.security.MessageDigest.getInstance("SHA-1")
      hash    <- ZIO(md.digest(data))
    } yield Chunk.fromArray(hash)
  }

  private def printBinaryString(data: Array[Byte]): String = {
    if (data.forall(b => b >= 32 && b <= 126)) {
      new String(data, StandardCharsets.US_ASCII)
    } else {
      data.map("%02x".format(_)).mkString
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

  def map(pairs: (BStr, BValue)*): BMap = BMap(pairs.toMap)

  implicit def stringToBStr(s: String): BStr = string(s)

  implicit class OptionBValueOps(opt: Option[BValue]) {
    def /(key: String): Option[BValue]     = opt.flatMap(bv => bv / key)
    def asChunk: Option[Chunk[Byte]]       = opt.flatMap(bv => bv.asChunk)
    def asString: Option[String]           = opt.flatMap(bv => bv.asString)
    def asLong: Option[Long]               = opt.flatMap(bv => bv.asInt)
    def asList: Option[List[BValue]]       = opt.flatMap(bv => bv.asList)
    def asStringList: Option[List[String]] = opt.flatMap(bv => bv.asStringList)
  }

  implicit val bStringOrdering: Ordering[BStr] = new Ordering[BStr] {

    private def compareChunks(pos: Int, a1: Chunk[Byte], a2: Chunk[Byte]): Int =
      if (pos < a1.length && pos < a2.length) {
        val c1 = a1(pos)
        val c2 = a2(pos)

        if (c1 == c2) compareChunks(pos + 1, a1, a2)
        else c1 - c2
      } else {
        a1.length - a2.length
      }

    override def compare(x: BStr, y: BStr): Int = compareChunks(0, x.value, y.value)
  }

}
