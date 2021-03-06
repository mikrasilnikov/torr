package torr.dispatcher

import torr.actorsystem.ActorSystem
import torr.consoleui.{ConsoleUI, SimpleProgressBar}
import torr.directbuffers.DirectBufferPool
import zio._
import zio.actors.ActorRef
import torr.dispatcher.Actor._
import torr.fileio.FileIO
import torr.metainfo.MetaInfo
import torr.peerwire.TorrBitSet
import zio.clock.Clock
import zio.console.{Console, putStr, putStrLn}
import zio.duration.durationInt
import zio.logging.Logging
import zio.nio.core.InetSocketAddress

case class DispatcherLive(private val actor: ActorRef[Command]) extends Dispatcher.Service {

  def acquireJob(peerId: PeerId): Task[AcquireJobResult] =
    actor ? AcquireJob(peerId)

  def releaseJob(peerId: PeerId, job: => ReleaseJobStatus): Task[Unit] =
    actor ! ReleaseJob(peerId, job)

  def acquireUploadSlot(peerId: PeerId): Task[AcquireUploadSlotResult] = actor ? AcquireUploadSlot(peerId)
  def releaseUploadSlot(peerId: PeerId): Task[Unit]                    = actor ? ReleaseUploadSlot(peerId)

  def isDownloadCompleted: Task[DownloadCompletion]      = actor ? IsDownloadCompleted
  def isRemoteInteresting(peerId: PeerId): Task[Boolean] = actor ? IsRemoteInteresting(peerId)

  def registerPeer(peerId: PeerId, address: InetSocketAddress): Task[Unit] = actor ? RegisterPeer(peerId, address)
  def unregisterPeer(peerId: PeerId): Task[Unit]                           = actor ? UnregisterPeer(peerId)

  def reportHave(peerId: PeerId, piece: PieceId): Task[Unit]           = actor ! ReportHave(peerId, piece)
  def reportHaveMany(peerId: PeerId, pieces: Set[PieceId]): Task[Unit] = actor ! ReportHaveMany(peerId, pieces)

  def peerIsSeeding(peerId: PeerId): Task[Boolean] = actor ? PeerIsSeeding(peerId)

  def reportDownloadSpeed(peerId: PeerId, bytesPerSecond: PieceId): Task[Unit] =
    actor ! ReportDownloadSpeed(peerId, bytesPerSecond)
  def reportUploadSpeed(peerId: PeerId, bytesPerSecond: PieceId): Task[Unit]   =
    actor ! ReportUploadSpeed(peerId, bytesPerSecond)

  def numUploadingPeers: Task[PieceId] = actor ? NumActivePeers

  def subscribeToHaveUpdates(peerId: PeerId): Task[(Set[PieceId], Dequeue[PieceId])] =
    actor ? SubscribeToHaveUpdates(peerId)

  def mapState[A](f: State => A): Task[A] = actor ? MapState(f)
}

object DispatcherLive {

  def make(maxDown: Int, maxUp: Int): ZLayer[
    ConsoleUI with ActorSystem with DirectBufferPool with FileIO with Logging with Console with Clock,
    Throwable,
    Dispatcher
  ] = {
    val effect = for {
      fileIO        <- ZIO.service[FileIO.Service].toManaged_
      metaInfo      <- fileIO.metaInfo.toManaged_
      filesAreFresh <- fileIO.freshFilesWereAllocated.toManaged_

      localHave <- (if (filesAreFresh)
                      ZIO.succeed(new Array[Boolean](metaInfo.pieceHashes.length))
                    else
                      for {
                        pieceRef    <- Ref.make[Int](0)
                        progressRef <- (updateProgress(pieceRef, metaInfo.pieceHashes.size) *>
                                         ZIO.sleep(1.second))
                                         .forever.fork
                        res         <- checkTorrent(metaInfo, pieceRef)
                        _           <- progressRef.interrupt.delay(1.second) *> putStrLn("")
                      } yield res).toManaged_

      actor     <- makeActor(
                     Actor.State(
                       metaInfo,
                       localHave,
                       maxSimultaneousDownloads = maxDown,
                       maxSimultaneousUploads = maxUp
                     )
                   )
                     .toManaged(actor =>
                       Logging.debug("dispatcher actor stopping") *>
                         actor.stop.orDie *>
                         Logging.debug("dispatcher actor stopped")
                     )

    } yield DispatcherLive(actor)

    effect.toLayer
  }

  private[dispatcher] def makeActor(state: Actor.State)
      : RIO[ConsoleUI with ActorSystem with Logging with Clock, ActorRef[Command]] = {
    val effect = for {
      system <- ZIO.service[ActorSystem.Service]

      supervisor = actors.Supervisor.retryOrElse(
                     Schedule.stop,
                     (t, _: Unit) =>
                       Logging.debug(s"Dispatcher failed with ${t.getClass}: ${t.getMessage}\n${t.toString}")
                   )

      actor     <- system.system.make(
                     "DispatcherLive",
                     supervisor,
                     state,
                     Actor.stateful
                   )
    } yield actor

    effect
  }

  private def checkTorrent(
      metaInfo: MetaInfo,
      pieceRef: Ref[Int]
  ): RIO[FileIO with DirectBufferPool with Clock, Array[Boolean]] = {
    for {
      actualHashes <- ZIO.foreach(metaInfo.pieceHashes.indices.toVector) { pieceId =>
                        Hasher.hashPiece(metaInfo, pieceId) <* pieceRef.set(pieceId + 1)
                      }
      result        = new Array[Boolean](metaInfo.pieceHashes.length)
      _             = metaInfo.pieceHashes.indices.foreach { i =>
                        if (actualHashes(i) == metaInfo.pieceHashes(i))
                          result(i) = true
                      }
    } yield result
  }

  private def updateProgress(currentRef: Ref[Int], total: Int): RIO[Console, Unit] = {
    for {
      current <- currentRef.get
      progress = current * 100 / total
      output   = SimpleProgressBar.render("Checking files  ", 50, progress)
      _       <- (putStr("\b" * output.length) *> putStr(output)).uninterruptible
    } yield ()
  }
}
