package torr.bencode

import zio._
import java.nio.charset.StandardCharsets
import torr.misc.Traverse
import scala.language.implicitConversions
import scala.annotation.tailrec

sealed trait BValue {

  def /(key: String): Option[BValue] = {
    this match {
      case BDict(map) => map.get(key)
      case _          => None
    }
  }

  def asLong: Option[Long] = {
    this match {
      case BLong(v) => Some(v)
      case _        => None
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
      case BDict(v) => Some(v)
      case _        => None
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

  def getSHA1: Chunk[Byte] = {
    val data = BEncode.write(this)
    val md   = java.security.MessageDigest.getInstance("SHA-1")
    val hash = md.digest(data.toArray)
    Chunk.fromArray(hash)
  }

}

final case class BLong(value: Long)              extends BValue
final case class BStr(value: Chunk[Byte])        extends BValue
final case class BList(value: List[BValue])      extends BValue
final case class BDict(value: Map[BStr, BValue]) extends BValue

object BValue {
  def string(value: String): BStr = BStr(Chunk.fromArray(value.getBytes(StandardCharsets.UTF_8)))

  def list(values: BValue*): BList = BList(values.toList)

  def long(value: Long): BLong = BLong(value)

  def map(pairs: (BStr, BValue)*): BDict = BDict(pairs.toMap)

  implicit def stringToBStr(s: String): BStr = string(s)

  implicit class OptionBValueOps(opt: Option[BValue]) {
    def /(key: String): Option[BValue]     = opt.flatMap(bv => bv / key)
    def asChunk: Option[Chunk[Byte]]       = opt.flatMap(bv => bv.asChunk)
    def asString: Option[String]           = opt.flatMap(bv => bv.asString)
    def asLong: Option[Long]               = opt.flatMap(bv => bv.asLong)
    def asInt: Option[Int]                 = opt.flatMap(bv => bv.asLong).map(_.toInt)
    def asList: Option[List[BValue]]       = opt.flatMap(bv => bv.asList)
    def asStringList: Option[List[String]] = opt.flatMap(bv => bv.asStringList)
  }

  implicit val bStringOrdering: Ordering[BStr] = new Ordering[BStr] {

    @tailrec
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
