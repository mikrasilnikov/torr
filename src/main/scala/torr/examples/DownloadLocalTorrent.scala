package torr.examples

import torr.actorsystem.ActorSystemLive
import torr.directbuffers.DirectBufferPoolLive
import torr.metainfo.MetaInfo
import torr.peerwire.{Message, PeerHandle}
import zio._
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.InetSocketAddress

object DownloadLocalTorrent extends App {

  val metaInfoFile     =
    "d:\\Torrents\\!torrent\\Уэллс Марта - Дневники Киллербота 05, Сетевой эффект [Князев Игорь, 2021, 128 kbps, MP3] [rutracker-6097721].torrent"
  val dstDirectoryName = "c:\\!temp\\CopyTest1\\"
  val remoteHost       = "localhost"
  val remotePort       = 57617

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val effect = for {
      peerHash      <- random.nextBytes(12)
      localPeerId    = Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
      remoteAddress <- InetSocketAddress.hostName(remoteHost, remotePort)
      metaInfo      <- MetaInfo.fromFile(metaInfoFile)
      _             <- PeerHandle.fromAddress(remoteAddress, metaInfo.infoHash, localPeerId)
                         .use(peerHandle => downloadProc(peerHandle))
    } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      Slf4jLogger.make((_, message) => message),
      DirectBufferPoolLive.make(16)
    ).exitCode
  }

  def downloadProc(peer: PeerHandle): ZIO[Any, Throwable, Unit] = {
    for {
      bitField <- peer.receive[Message.BitField].debug("received")
      msg      <- receiveBitfield(peer).debug("received")
      _        <- peer.send(Message.Interested)
    } yield ()
  }

  def receiveBitfield(peer: PeerHandle): Task[Unit] = {
    for {
      msg <- peer.receive[Message.BitField, Message.Have].debug("receiveBitfield")
      res <- msg match {
               case Message.BitField(_) => receiveBitfield(peer)
               case Message.Have(_)     => receiveBitfield(peer)
               case _                   => ZIO.succeed(msg)
             }
    } yield ???
  }

  def skipHave(peer: PeerHandle): Task[Unit] = {
    for {
      msg <- peer.receive[Message.Unchoke.type, Message.Have].debug("skipHave")
      res <- msg match {
               case Message.Have(_) => skipHave(peer)
               case _               => ZIO.succeed(msg)
             }
    } yield res
  }

}
