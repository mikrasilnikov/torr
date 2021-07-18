package torr.trackers

import torr.bencode._
import torr.misc.Traverse
import java.nio.ByteBuffer

case class Peer(ip: String, port: Int, peerId: Option[Array[Byte]] = None)
case class TrackerResponse(peers: List[Peer], interval: Int)

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
          } yield Peer(ip, port.toInt, Some(peerId.toArray))
        }
        Traverse.sequence(peerOptions)

      // Compact response
      case BStr(data)    =>
        if (data.size % 6 != 0)
          None
        else {
          val res = data.sliding(6, 6).map { item =>
            val ip   = s"${item(0)}.${item(1)}.${item(2)}.${item(3)}"
            val port = ByteBuffer.wrap(item.slice(4, 6).toArray).getShort
            Peer(ip, port)
          }
          Some(res.toList)
        }
    }
  }
}
