package torr.announce

import torr.bencode._
import torr.misc.Traverse
import zio.Chunk

import java.nio.ByteBuffer

case class TrackerResponse(peers: List[Peer], interval: Int)

case class Peer(ip: String, port: Int, peerId: Option[Chunk[Byte]] = None) {
  override def equals(obj: Any): Boolean =
    obj match {
      case that @ Peer(_, _, _) => that.ip == ip && that.port == port
      case _                    => false
    }
  override def hashCode(): Int           = (ip, port).hashCode()
}

object TrackerResponse {

  def interpret(bValue: BValue): Option[TrackerResponse] = {
    for {
      interval <- (bValue / "interval").asLong
      peers    <- (bValue / "peers").flatMap(interpretPeers)
    } yield TrackerResponse(peers, interval.toInt)
  }

  def interpretPeers(bPeers: BValue): Option[List[Peer]] = {
    bPeers match {

      // Long response
      case BList(values) =>
        val peerOptions = values.map { v =>
          for {
            ip     <- (v / "ip").asString
            port   <- (v / "port").asLong
            peerId <- (v / "peer id").asChunk
          } yield Peer(ip, port.toInt, Some(peerId))
        }
        Traverse.sequence(peerOptions)

      // Compact response
      case BStr(data)    =>
        if (data.size % 6 != 0)
          None
        else {
          val res = data.sliding(6, 6).map { item =>
            val ip   = s"${unsigned(item(0))}.${unsigned(item(1))}.${unsigned(item(2))}.${unsigned(item(3))}"
            val port = unsigned(ByteBuffer.wrap(item.slice(4, 6).toArray).getShort)
            Peer(ip, port)
          }
          Some(res.toList)
        }

      case _             => None
    }
  }

  def unsigned(b: Byte): Int = {
    val buf = ByteBuffer.allocate(4)
    buf.put(3, b)
    buf.getInt
  }

  def unsigned(s: Short): Int = {
    val buf = ByteBuffer.allocate(4)
    buf.putShort(2, s)
    buf.getInt
  }

}
