package torr.peerwire

import zio._
import zio.nio.core.{Buffer, ByteBuffer}
import torr.channels.ByteChannel
import torr.directbuffers.DirectBufferPool
import zio.clock.Clock
import java.nio.charset.StandardCharsets

sealed trait Message {
  def send(channel: ByteChannel, buf: ByteBuffer): Task[Unit] =
    Message.send(this, channel, buf)
}

object MessageTypes {
  type KeepAlive = Message.KeepAlive.type
  type Have      = Message.Have
  type BitField  = Message.BitField
  type Port      = Message.Port
  type Request   = Message.Request
  type Cancel    = Message.Cancel
  type Piece     = Message.Piece
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

  case class Request(index: Int, offset: Int, length: Int)         extends Message
  case class Cancel(index: Int, offset: Int, length: Int)          extends Message
  case class Piece(index: Int, offset: Int, block: ByteBuffer)     extends Message
  case class Handshake(infoHash: Chunk[Byte], peerId: Chunk[Byte]) extends Message
  private[peerwire] case object Fail                               extends Message

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
  private val FailMessageId: Byte      = Byte.MaxValue

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
      case Handshake(infoHash, peerId)   => sendHandshake(infoHash, peerId, channel, buf)
      case Fail                          => ???
    }

  def receive(channel: ByteChannel, buf: ByteBuffer): RIO[DirectBufferPool with Clock, Message] = {
    for {
      len <- receiveAmount(channel, buf, 4) *> buf.getInt
      res <- len match {
               case 0 => ZIO.succeed(Message.KeepAlive)
               case _ =>
                 for {
                   id  <- receiveAmount(channel, buf, 1) *> buf.get
                   res <- id match {
                            case ChokeMsgId         => ZIO.succeed(Choke)
                            case UnchokeMsgId       => ZIO.succeed(Unchoke)
                            case InterestedMsgId    => ZIO.succeed(Interested)
                            case NotInterestedMsgId => ZIO.succeed(NotInterested)
                            case HaveMsgId          => receiveHave(channel, buf)
                            case RequestMsgId       => receiveRequest(channel, buf)
                            case CancelMsgId        => receiveCancel(channel, buf)
                            case PortMsgId          => receivePort(channel, buf)
                            case BitfieldMsgId      => receiveBitField(channel, payloadLength = len - 1)
                            case PieceMsgId         => receivePiece(channel, payloadLength = len - 1)
                            case FailMessageId      => ZIO.succeed(Message.Fail)
                            case _                  => ZIO.fail(new Exception(s"Unknown message id $id"))
                          }
                 } yield res
             }
    } yield res
  }

  def sendPiece(index: Int, begin: Int, block: ByteBuffer, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    for {
      _        <- buf.clear
      dataSize <- block.remaining
      _        <- buf.putInt(9 + dataSize)
      _        <- buf.put(PieceMsgId)
      _        <- buf.putInt(index)
      _        <- buf.putInt(begin)
      _        <- buf.flip *> writeWhole(channel, buf)
      _        <- writeWhole(channel, block)
    } yield ()
  }

  def receivePiece(
      channel: ByteChannel,
      payloadLength: Int
  ): RIO[DirectBufferPool with Clock, Piece] = {
    for {
      payloadBuf <- DirectBufferPool.allocate
      _          <- receiveAmount(channel, payloadBuf, payloadLength)
      index      <- payloadBuf.getInt
      begin      <- payloadBuf.getInt
    } yield Piece(index, begin, payloadBuf)
  }

  def sendCancel(index: Int, begin: Int, length: Int, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
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

  def receiveCancel(channel: ByteChannel, buf: ByteBuffer): Task[Cancel] = {
    for {
      _      <- receiveAmount(channel, buf, 3 * 4)
      index  <- buf.getInt
      begin  <- buf.getInt
      length <- buf.getInt
    } yield Cancel(index, begin, length)
  }

  def sendRequest(index: Int, begin: Int, length: Int, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
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

  def receiveRequest(channel: ByteChannel, buf: ByteBuffer): Task[Request] = {
    for {
      _      <- receiveAmount(channel, buf, 3 * 4)
      index  <- buf.getInt
      begin  <- buf.getInt
      length <- buf.getInt
    } yield Request(index, begin, length)
  }

  def sendPort(port: Int, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
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

  def receivePort(channel: ByteChannel, buf: ByteBuffer): Task[Port] = {
    for {
      _       <- receiveAmount(channel, buf, 2)
      portBuf <- Buffer.byte(4)
      _       <- portBuf.position(2)
      _       <- portBuf.putByteBuffer(buf)
      _       <- portBuf.flip
      value   <- portBuf.asIntBuffer.flatMap(_.get)
    } yield Port(value)
  }

  def sendHave(pieceIndex: Int, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    for {
      _ <- buf.clear
      _ <- buf.putInt(5)
      _ <- buf.put(HaveMsgId)
      _ <- buf.putInt(pieceIndex)
      _ <- buf.flip *> writeWhole(channel, buf)
    } yield ()
  }

  def receiveHave(channel: ByteChannel, buf: ByteBuffer): Task[Have] = {
    for {
      _          <- receiveAmount(channel, buf, 4)
      pieceIndex <- buf.getInt
    } yield Have(pieceIndex)
  }

  def sendBitField(set: TorrBitSet, channel: ByteChannel, buf: ByteBuffer): Task[Unit] = {
    for {
      _     <- buf.clear
      chunk  = set.toBytes
      msgLen = chunk.length + 1
      _     <- buf.putInt(msgLen) *> buf.put(BitfieldMsgId)
      _     <- buf.flip *> writeWhole(channel, buf)
      _     <- writeWhole(chunk, channel, buf)
    } yield ()
  }

  def receiveBitField(channel: ByteChannel, payloadLength: Int): Task[BitField] = {
    for {
      payloadBuf <- Buffer.byte(payloadLength)
      _          <- receiveAmount(channel, payloadBuf, payloadLength)
      chunk      <- payloadBuf.getChunk()
      bitSet      = TorrBitSet.fromBytes(chunk)
    } yield BitField(bitSet)
  }

  def sendHandshake(
      infoHash: Chunk[Byte],
      peerId: Chunk[Byte],
      channel: ByteChannel,
      buf: ByteBuffer
  ): Task[Unit] = {
    val reserved = Chunk.fill(8)(0.toByte)
    for {
      _   <- buf.clear
      str  = Chunk.fromArray("BitTorrent protocol".getBytes(StandardCharsets.US_ASCII))
      _   <- buf.put(str.length.toByte)
      _   <- buf.putChunk(str)
      _   <- buf.putChunk(reserved)
      _   <- buf.putChunk(infoHash)
      _   <- buf.putChunk(peerId)
      _   <- buf.flip
      len <- buf.limit
      _   <- ZIO.fail(new IllegalStateException(s"Wrong handshake length $len")).unless(len == 19 + 49)
      _   <- writeWhole(channel, buf)
    } yield ()
  }

  def receiveHandshake(channel: ByteChannel, buf: ByteBuffer): Task[Handshake] = {
    for {
      _        <- receiveAmount(channel, buf, 1)
      strLen   <- buf.get
      _        <- ZIO.fail(new IllegalStateException(s"Unexpected strLen = $strLen during handshake"))
                    .unless(strLen == 19)
      str      <- receiveAmount(channel, buf, strLen) *> buf.getChunk()
      _        <- ZIO.fail(new IllegalStateException(s"Unexpected protocol name")).unless(
                    str == Chunk.fromArray("BitTorrent protocol".getBytes(StandardCharsets.US_ASCII))
                  )
      _        <- receiveAmount(channel, buf, 8) *> buf.getChunk()
      infoHash <- receiveAmount(channel, buf, 20) *> buf.getChunk()
      peerId   <- receiveAmount(channel, buf, 20) *> buf.getChunk()
    } yield Handshake(infoHash, peerId)
  }

  def sendBytes(channel: ByteChannel, buf: ByteBuffer, values: Byte*): Task[Unit] = {
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

  private def receiveAmount(channel: ByteChannel, buf: ByteBuffer, amount: Int): Task[Unit] = {

    //noinspection SimplifyUnlessInspection
    def fillBuf: Task[Unit] =
      for {
        rem <- buf.remaining
        _   <- rem match {
                 case 0 => ZIO.unit
                 case _ => channel.read(buf) *> fillBuf
               }
      } yield ()

    for {
      _ <- buf.clear
      _ <- buf.limit(amount)
      _ <- fillBuf
      _ <- buf.flip
    } yield ()
  }
}
