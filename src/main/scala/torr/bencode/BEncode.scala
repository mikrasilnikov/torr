package torr.bencode

import zio._
import java.nio.charset.StandardCharsets
import scala.annotation.tailrec

object BEncode {

  def write(bVal: BValue): Chunk[Byte] =
    bVal match {
      case long @ BLong(_) => writeLong(long)
      case str @ BStr(_)   => writeString(str)
      case lst @ BList(_)  => writeList(lst)
      case dict @ BDict(_) => writeDict(dict)
    }

  private def writeLong(long: BLong): Chunk[Byte] = {
    val data = s"i${long.value}e".getBytes(StandardCharsets.UTF_8)
    Chunk.fromArray(data)
  }

  private def writeString(str: BStr): Chunk[Byte] = {
    val size = s"${str.value.length}:".getBytes(StandardCharsets.UTF_8)
    Chunk.fromArray(size) ++ str.value
  }

  private def writeList(lst: BList): Chunk[Byte] = {
    @tailrec
    def loop(elems: List[BValue], acc: Chunk[Byte] = Chunk.empty): Chunk[Byte] =
      elems match {
        case Nil    => acc
        case h :: t => loop(t, acc ++ write(h))
      }

    val prefix = Chunk.fromArray("l".getBytes(StandardCharsets.UTF_8))
    val suffix = Chunk.fromArray("e".getBytes(StandardCharsets.UTF_8))

    prefix ++ loop(lst.value) ++ suffix
  }

  private def writeDict(dict: BDict, acc: Chunk[Byte] = Chunk.empty): Chunk[Byte] = {

    def loop(kvps: List[(BStr, BValue)], acc: Chunk[Byte] = Chunk.empty): Chunk[Byte] = {
      kvps match {
        case Nil    => acc
        case h :: t => loop(t, acc ++ write(h._1) ++ write(h._2))
      }
    }

    val prefix = Chunk.fromArray("d".getBytes(StandardCharsets.UTF_8))
    val suffix = Chunk.fromArray("e".getBytes(StandardCharsets.UTF_8))

    prefix ++ loop(dict.value.toList.sortBy { case (key, _) => key }) ++ suffix
  }

  def read(data: Chunk[Byte]): BValue = {
    val (result, _) = readCore(data)
    result
  }

  def readCore(data: Chunk[Byte]): (BValue, Chunk[Byte]) =
    data.head match {
      case 'i' => readLong(data.drop(1))
      case 'l' => readList(data.drop(1))
      case 'd' => readDictionary(data.drop(1))
      case _   => readString(data)
    }

  @tailrec
  private def readLong(
      data: Chunk[Byte],
      endMark: Char = 'e',
      acc: StringBuilder = new StringBuilder
  ): (BLong, Chunk[Byte]) =
    data.head match {
      case `endMark` => (BLong(acc.toString.toLong), data.drop(1))
      case c         =>
        acc.append(c.toChar)
        readLong(data.tail, endMark, acc)
    }

  private def readString(data: Chunk[Byte]): (BStr, Chunk[Byte]) = {
    val (strLen, rem) = readLong(data, ':')
    val strData       = rem.take(strLen.value.toInt)
    (BStr(strData), rem.drop(strLen.value.toInt))
  }

  @tailrec
  private def readDictionary(data: Chunk[Byte], acc: Map[BStr, BValue] = Map.empty): (BDict, Chunk[Byte]) = {
    data.head match {
      case 'e' => (BDict(acc), data.drop(1))
      case _   =>
        val (key, rem1)   = readString(data)
        val (value, rem2) = readCore(rem1)
        readDictionary(rem2, acc + (key -> value))
    }
  }

  @tailrec
  private def readList(data: Chunk[Byte], acc: List[BValue] = Nil): (BList, Chunk[Byte]) = {
    data.head match {
      case 'e' => (BList(acc.reverse), data.drop(1))
      case _   =>
        val (elem, rem) = readCore(data)
        readList(rem, elem :: acc)
    }
  }
}
