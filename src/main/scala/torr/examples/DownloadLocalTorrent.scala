package torr.examples

import torr.actorsystem.ActorSystemLive
import torr.directbuffers.{DirectBufferPool, DirectBufferPoolLive}
import torr.metainfo.MetaInfo
import torr.peerwire.{Message, PeerHandle}
import torr.peerwire.MessageTypes._
import zio._
import zio.console.{Console, putStrLn}
import zio.logging.slf4j.Slf4jLogger
import zio.magic.ZioProvideMagicOps
import zio.nio.core.{InetAddress, InetSocketAddress}

import scala.collection.immutable.HashMap

object DownloadLocalTorrent extends App {

  val metaInfoFile          =
    "d:\\Torrents\\!torrent\\Breaking Bad - Season 1 [BDRip] (Кубик в Кубе).torrent"
  val dstDirectoryName      = "c:\\!temp\\CopyTest1\\"
  val remoteHost            = "localhost"
  val remotePort: Int       = 57617
  val requestSize: Int      = 16 * 1024
  val maxConcurrentRequests = 64

  sealed trait DownloadState
  case class InProgress(offset: Long, pendingRequests: Map[Long, Message.Request]) extends DownloadState
  case object Completed                                                            extends DownloadState

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val effect = for {
      peerHash      <- random.nextBytes(12)
      localPeerId    = Chunk.fromArray("-AZ2060-".getBytes) ++ peerHash
      //remoteAddress <- InetSocketAddress.hostName(remoteHost, remotePort)
      remoteAddress <- InetSocketAddress.hostName("173.212.205.73", 51413)
      metaInfo      <- MetaInfo.fromFile(metaInfoFile)
      _             <- PeerHandle.fromAddress(remoteAddress, metaInfo.infoHash, localPeerId)
                         .use(peerHandle => downloadProc(metaInfo, peerHandle))
    } yield ()

    effect.injectCustom(
      ActorSystemLive.make("Test"),
      Slf4jLogger.make((_, message) => message),
      DirectBufferPoolLive.make(128)
    ).exitCode
  }

  def downloadProc(metaInfo: MetaInfo, peer: PeerHandle): ZIO[DirectBufferPool with Console, Throwable, Unit] = {
    for {
      _          <- peer.receive[BitField].debug("rcv downloadProc")
      rcvHaveFib <- receiveHave(peer).fork
      _          <- peer.send(Message.Interested).debug("snd Interested")
      _          <- downloadUntilCompleted(metaInfo, peer)
      _          <- rcvHaveFib.interrupt
    } yield ()
  }

  def downloadUntilCompleted(
      metaInfo: MetaInfo,
      peer: PeerHandle,
      offset: Long = 0L,
      retryRequests: Map[Long, Message.Request] = HashMap.empty[Long, Message.Request]
  ): RIO[DirectBufferPool with Console, Unit] = {

    val torrentSize = metaInfo.entries.map(_.size).sum

    for {
      _     <- peer.receive[Unchoke].debug("rcv [downloadUntilCompleted]")
      _     <- ZIO.foreach_(retryRequests) {
                 case (_, req) => peer.send(req) //.debug(s"snd [downloadUntilCompleted] $req")
               }
      state <- downloadUntilChoked(peer, metaInfo, torrentSize, offset, retryRequests).debug("state")
      _     <- state match {
                 case Completed           => ZIO.unit
                 case InProgress(o, reqs) => downloadUntilCompleted(metaInfo, peer, o, reqs)
               }
    } yield ()
  }

  /** Downloads torrent from given `offset` until
    *  - completion or
    *  - being `Choked` by remote peer
    */
  def downloadUntilChoked(
      peer: PeerHandle,
      metaInfo: MetaInfo,
      torrentSize: Long,
      offset: Long,
      pendingRequests: Map[Long, Message.Request]
  ): RIO[Console with DirectBufferPool, DownloadState] = {
    val remaining = torrentSize - offset

    if (remaining <= 0 && pendingRequests.isEmpty) {

      ZIO.succeed(Completed)

    } else if (remaining > 0 && pendingRequests.size < maxConcurrentRequests) {
      // We must maintain a queue of unfulfilled requests for performance reasons.
      // See https://wiki.theory.org/BitTorrentSpecification#Queuing
      val amount   = math.min(requestSize.toLong, remaining)
      val req      = metaInfo.requestFromOffset(offset, amount.toInt)
      val progress = offset * 100 / torrentSize

      for {
        _   <- peer.send(req) // .debug(s"snd [downloadUntilChoked] $req")
        _   <- putStrLn(s"offset = ${offset / (1024 * 1024)}M, $progress% completed").when(
                 offset % (128L * 1024 * 1024) == 0
               )
        res <- downloadUntilChoked(peer, metaInfo, torrentSize, offset + amount, pendingRequests + (offset -> req))
      } yield res

    } else {
      for {
        msg <- peer.receive[Piece, Choke] //.debug("rcv [downloadUntilChoked]")
        res <- msg match {

                 case Message.Piece(index, begin, buf) =>
                   val rcvPieceOffset = index.toLong * metaInfo.pieceSize + begin
                   for {
                     _   <- DirectBufferPool.free(buf)
                     res <- downloadUntilChoked(peer, metaInfo, torrentSize, offset, pendingRequests - rcvPieceOffset)
                   } yield res

                 case Message.Choke                    => ZIO.succeed(InProgress(offset, pendingRequests))
                 case _                                => ZIO.dieMessage("!")
               }
      } yield res
    }
  }

  def receiveHave(peer: PeerHandle): Task[Unit] = {
    for {
      _ <- peer.receive[Have].debug("rcv")
      _ <- receiveHave(peer)
    } yield ()
  }

}
