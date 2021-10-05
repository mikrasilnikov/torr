package torr.dispatcher

import torr.actorsystem.ActorSystem
import torr.consoleui.{ConsoleUI, SimpleProgressBar}
import torr.directbuffers.DirectBufferPool
import zio._
import zio.actors.ActorRef
import torr.dispatcher.Actor._
import torr.fileio.FileIO
import torr.metainfo.MetaInfo
import zio.clock.Clock
import zio.console.{Console, putStr, putStrLn}
import zio.duration.durationInt

case class DispatcherLive(private val actor: ActorRef[Command]) extends Dispatcher.Service {

  def acquireJob(peerId: PeerId): Task[AcquireJobResult] = {
    for {
      p   <- Promise.make[Throwable, AcquireJobResult]
      _   <- actor ? AcquireJob(peerId, p)
      res <- p.await
    } yield res
  }

  def releaseJob(peerId: PeerId, job: => ReleaseJobStatus): Task[Unit] =
    actor ! ReleaseJob(peerId, job)

  def isDownloadCompleted: Task[Boolean]                 = actor ? IsDownloadCompleted
  def isRemoteInteresting(peerId: PeerId): Task[Boolean] = actor ? IsRemoteInteresting(peerId)

  def registerPeer(peerId: PeerId): Task[Unit]   = actor ! RegisterPeer(peerId)
  def unregisterPeer(peerId: PeerId): Task[Unit] = actor ! UnregisterPeer(peerId)

  def reportHave(peerId: PeerId, piece: PieceId): Task[Unit]           = actor ! ReportHave(peerId, piece)
  def reportHaveMany(peerId: PeerId, pieces: Set[PieceId]): Task[Unit] = actor ! ReportHaveMany(peerId, pieces)

  def reportDownloadSpeed(peerId: PeerId, bytesPerSecond: PieceId): Task[Unit] =
    actor ! ReportDownloadSpeed(peerId, bytesPerSecond)
  def reportUploadSpeed(peerId: PeerId, bytesPerSecond: PieceId): Task[Unit]   =
    actor ! ReportUploadSpeed(peerId, bytesPerSecond)
}

object DispatcherLive {

  def make: ZLayer[
    ConsoleUI with ActorSystem with DirectBufferPool with FileIO with Clock with Console,
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

      actor     <- makeActor(Actor.State(metaInfo, localHave)).toManaged(actor => actor.stop.orDie)
      uiFiber   <- drawConsoleUI(actor).fork.toManaged(fib => fib.interrupt *> ZIO(println("progress interrupted")).orDie)

    } yield DispatcherLive(actor)

    effect.toLayer

  }

  private[dispatcher] def makeActor(state: Actor.State)
      : RIO[ConsoleUI with ActorSystem with Clock, ActorRef[Command]] = {
    val effect = for {
      system <- ZIO.service[ActorSystem.Service]
      actor  <- system.system.make(
                  "DispatcherLive",
                  actors.Supervisor.none,
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

  private def drawConsoleUI(actor: ActorRef[Command]): RIO[Clock, Unit] = {
    for {
      _ <- actor ! DrawProgress
      _ <- drawConsoleUI(actor).delay(1.second)
    } yield ()
  }

}
