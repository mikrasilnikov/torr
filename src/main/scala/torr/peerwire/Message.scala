package torr.peerwire

import zio._
import zio.nio.core.{Buffer, ByteBuffer}
import torr.channels.ByteChannel

import scala.collection.mutable.BitSet

sealed trait Message {
  def send(channel: ByteChannel, buf: ByteBuffer): Task[Unit] =
    Message.send(this, channel, buf)
}

object Message {
  case object KeepAlive                 extends Message
  case object Choke                     extends Message
  case object Unchoke                   extends Message
  case object Interested                extends Message
  case object NotInterested             extends Message
  case class Have(pieceIndex: Int)      extends Message
  case class BitField(bits: TorrBitSet) extends Message
  case class Port(listenPort: Int)      extends Message

  case class Request(index: Int, begin: Int, length: Int)    extends Message
  case class Cancel(index: Int, begin: Int, length: Int)     extends Message
  case class Piece(index: Int, begin: Int, data: ByteBuffer) extends Message

  private val ChokeMsgId: Byte         = 0
  private val UnchokeMsgId: Byte       = 1
  private val InterestedMsgId: Byte    = 2
  private val NotInterestedMsgId: Byte = 3
  private val HaveMsgId: Byte          = 4
  private val BitfieldMsgId: Byte      = 5
  private val RequestMsgId: Byte       = 6
  private val PieceMsgId: Byte         = 7
  private val CancelMsgId: Byte        = 8
  private val PortMsgId: Byte          = 9

  def send(message: Message, channel: ByteChannel, bufSize: Int = 1024 * 4): Task[Unit] =
    for {
      buf <- Buffer.byte(bufSize)
      _   <- send(message, channel, buf)
    } yield ()

  def send(message: Message, channel: ByteChannel, buf: ByteBuffer): Task[Unit] =
    message match {
      case KeepAlive                     => sendBytes(channel, buf)
      case Choke                         => sendBytes(channel, buf, ChokeMsgId)
      case Unchoke                       => sendBytes(channel, buf, UnchokeMsgId)
      case Interested                    => sendBytes(channel, buf, InterestedMsgId)
      case NotInterested                 => sendBytes(channel, buf, NotInterestedMsgId)
      case Have(pieceIndex)              => sendHave(pieceIndex, channel, buf)
      case BitField(bits)                => sendBitField(bits, channel, buf)
      case Port(port)                    => sendPort(port, channel, buf)
      case Request(index, begin, length) => sendRequest(index, begin, length, channel, buf)
      case Cancel(index, begin, length)  => sendCancel(index, begin, length, channel, buf)
      case Piece(index, begin, data)     => sendPiece(index, begin, data, channel, buf)
    }

  def sendPiece(index: Int, begin: Int, data: ByteBuffer, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    assert(buf.capacity >= 9)
    for {
      _        <- buf.clear
      dataSize <- data.remaining
      _        <- buf.putInt(9 + dataSize)
      _        <- buf.put(PieceMsgId)
      _        <- buf.putInt(index)
      _        <- buf.putInt(begin)
      _        <- buf.flip *> writeWhole(channel, buf)
      _        <- writeWhole(channel, data)
    } yield ()
  }

  def sendCancel(index: Int, begin: Int, length: Int, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    assert(buf.capacity >= 13 + 4)
    for {
      _ <- buf.clear
      _ <- buf.putInt(13)
      _ <- buf.put(CancelMsgId)
      _ <- buf.putInt(index)
      _ <- buf.putInt(begin)
      _ <- buf.putInt(length)
      _ <- buf.flip *> writeWhole(channel, buf)
    } yield ()
  }

  def sendRequest(index: Int, begin: Int, length: Int, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    assert(buf.capacity >= 13 + 4)
    for {
      _ <- buf.clear
      _ <- buf.putInt(13)
      _ <- buf.put(RequestMsgId)
      _ <- buf.putInt(index)
      _ <- buf.putInt(begin)
      _ <- buf.putInt(length)
      _ <- buf.flip *> writeWhole(channel, buf)
    } yield ()
  }

  def sendPort(port: Int, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    assert(buf.capacity >= 7)
    for {
      _       <- buf.clear
      _       <- buf.putInt(3)
      _       <- buf.put(PortMsgId)
      // Since there is no unsigned short type in Scala we need to copy
      // two lower bytes from `port` parameter to the output buffer.
      portBuf <- Buffer.byte(4)
      _       <- portBuf.asIntBuffer.flatMap(_.put(port))
      _       <- portBuf.position(2)
      _       <- buf.putByteBuffer(portBuf)
      _       <- buf.flip *> writeWhole(channel, buf)
    } yield ()
  }

  def sendHave(pieceIndex: Int, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    assert(buf.capacity >= 9)
    for {
      _ <- buf.clear
      _ <- buf.putInt(5)
      _ <- buf.put(HaveMsgId)
      _ <- buf.putInt(pieceIndex)
      _ <- buf.flip *> writeWhole(channel, buf)
    } yield ()
  }

  def sendBitField(set: TorrBitSet, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    assert(buf.capacity >= 5)
    for {
      _     <- buf.clear
      chunk  = set.toBytes
      msgLen = chunk.length + 1
      _     <- buf.putInt(msgLen) *> buf.put(BitfieldMsgId)
      _     <- buf.flip *> writeWhole(channel, buf)
      _     <- writeWhole(chunk, channel, buf)
    } yield ()
  }

  def sendBytes(channel: ByteChannel, buf: ByteBuffer, values: Byte*): Task[Unit] = {
    assert(buf.capacity >= values.length)
    for {
      _ <- buf.clear
      _ <- buf.putInt(values.length)
      _ <- ZIO.foreach_(values)(v => buf.put(v))
      _ <- buf.flip *> writeWhole(channel, buf)
    } yield ()
  }

  private def writeWhole(channel: ByteChannel, buf: ByteBuffer): Task[Unit] =
    buf.hasRemaining.flatMap {
      case false => ZIO.unit
      case _     => channel.write(buf) *> writeWhole(channel, buf)
    }

  def writeWhole(chunk: Chunk[Byte], channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    val rem1 = for {
      _   <- buf.clear
      res <- buf.putChunk(chunk)
      _   <- buf.flip *> writeWhole(channel, buf)
    } yield res

    rem1.flatMap {
      case Chunk.empty => ZIO.unit
      case r           => writeWhole(r, channel, buf)
    }
  }

  private def peek(buf: ByteBuffer): Task[Byte] = buf.position.flatMap(pos => buf.get(pos))

}