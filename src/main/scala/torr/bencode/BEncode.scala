package torr.bencode

import zio._
import zio.nio.core._
import torr.channels.ByteChannel

import java.nio.charset.StandardCharsets

object BEncode {

  def read(channel: ByteChannel, bufSize: Int = 64 * 1024): Task[BValue] = {
    for {
      buf <- Buffer.byte(bufSize)
      _   <- readMore(channel, buf)
      res <- read(channel, buf)
    } yield res
  }

  def read(channel: ByteChannel, buf: ByteBuffer): Task[BValue] = {

    val next = for {
      _   <- readMore(channel, buf).unlessM(buf.hasRemaining)
      res <- peek(buf)
    } yield res

    next.flatMap {
      case 'i' => buf.get *> readInt(channel, buf, 'e')
      case 'l' => buf.get *> readList(channel, buf)
      case 'd' => buf.get *> readDictionary(channel, buf)
      case _   => readString(channel, buf)
    }
  }

  def write(bVal: BValue, channel: ByteChannel, bufSize: Int = 64 * 1024): Task[Unit] = {
    for {
      buf <- Buffer.byte(bufSize)
      _   <- write(bVal, channel, buf)
    } yield ()
  }

  def write(bVal: BValue, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    bVal match {
      case bStr @ BStr(_) => writeString(bStr, channel, buf)
    }
  }

  private def readList(channel: ByteChannel, buf: ByteBuffer): Task[BList] = {
    //noinspection SimplifyZipRightToSucceedInspection
    def loop(acc: List[BValue]): Task[BList] = {

      val nextChar = for {
        _   <- readMore(channel, buf).unlessM(buf.hasRemaining)
        res <- peek(buf)
      } yield res

      nextChar.flatMap {
        case 'e' => buf.get *> ZIO.succeed(BList(acc.reverse))
        case _   => read(channel, buf).flatMap(bVal => loop(bVal :: acc))
      }
    }

    loop(Nil)
  }

  private def readDictionary(channel: ByteChannel, buf: ByteBuffer): Task[BMap] = {
    //noinspection SimplifyZipRightToSucceedInspection
    def loop(acc: List[(BStr, BValue)]): Task[BMap] = {
      val next = for {
        _   <- readMore(channel, buf).unlessM(buf.hasRemaining)
        res <- peek(buf)
      } yield res

      next.flatMap {
        case 'e' => buf.get *> ZIO.succeed(BMap(Map.from(acc)))
        case _   =>
          val readKvp = for {
            key   <- readString(channel, buf)
            value <- read(channel, buf)
          } yield (key, value)

          readKvp.flatMap(kvp => loop(kvp :: acc))
      }
    }

    loop(Nil)
  }

  private def readInt(channel: ByteChannel, buf: ByteBuffer, endMark: Char): Task[BInt] = {
    val builder = new StringBuilder

    def loop: Task[String] = {
      val readChar = for {
        _ <- readMore(channel, buf).unlessM(buf.hasRemaining)
        c <- buf.get
      } yield c.toChar

      readChar.flatMap {
        case `endMark` => ZIO.succeed(builder.toString)
        case c         => ZIO(builder.append(c)) *> loop
      }
    }

    for {
      str <- loop
      int <- ZIO(str.toLong)
    } yield BInt(int)
  }

  private def readString(channel: ByteChannel, buf: ByteBuffer): Task[BStr] = {

    def loop(acc: Chunk[Byte], bytesToRead: Int): Task[BStr] =
      bytesToRead match {
        case 0 => ZIO.succeed(BStr(acc))
        case _ =>
          val readChunk = for {
            _     <- readMore(channel, buf).unlessM(buf.hasRemaining)
            rem   <- buf.remaining
            chunk <- buf.getChunk(math.min(rem, bytesToRead))
          } yield chunk

          readChunk.flatMap(chunk => loop(acc ++ chunk, bytesToRead - chunk.size))
      }

    for {
      size <- readInt(channel, buf, ':')
      res  <- loop(Chunk.empty, size.value.toInt)
    } yield res
  }

  private def writeString(str: BStr, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {

    def loop(rem: Chunk[Byte]): Task[Unit] = {
      val rem1 = for {
        _   <- buf.clear
        res <- buf.putChunk(rem)
        _   <- buf.flip *> writeAll(channel, buf)
      } yield res

      rem1.flatMap {
        case Chunk.empty => ZIO.unit
        case r           => loop(r)
      }
    }

    for {
      _     <- buf.clear
      prefix = s"${str.value.size.toString}:".getBytes(StandardCharsets.UTF_8)
      _     <- loop(Chunk.fromArray(prefix) ++ str.value)
    } yield ()

  }

  private def readMore(channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    for {
      _ <- buf.clear
      _ <- channel.read(buf)
      _ <- buf.flip
    } yield ()
  }

  def writeAll(channel: ByteChannel, buf: ByteBuffer): Task[Unit] =
    buf.hasRemaining.flatMap {
      case false => ZIO.unit
      case _     => channel.write(buf) *> writeAll(channel, buf)
    }

  private def peek(buf: ByteBuffer): Task[Byte] = buf.position.flatMap(pos => buf.get(pos))
}
