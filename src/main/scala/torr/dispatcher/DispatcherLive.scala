package torr.dispatcher

import torr.actorsystem.ActorSystem
import torr.consoleui.SimpleProgressBar
import torr.directbuffers.DirectBufferPool
import zio._
import zio.actors.ActorRef
import torr.dispatcher.Actor.{AcquireJob, Command, IsDownloadCompleted, IsRemoteInteresting, ReleaseJob}
import torr.fileio.FileIO
import torr.metainfo.MetaInfo
import zio.clock.Clock
import zio.console.{Console, putStr, putStrLn}
import zio.duration.durationInt
import scala.collection.mutable

case class DispatcherLive(private val actor: ActorRef[Command]) extends Dispatcher.Service {

  def acquireJob(peerId: PeerId, remoteHave: Set[PieceId]): Task[AcquireJobResult] = {
    for {
      p   <- Promise.make[Throwable, AcquireJobResult]
      _   <- actor ? AcquireJob(peerId, remoteHave, p)
      res <- p.await
    } yield res
  }

  def releaseJob(peerId: PeerId, job: => ReleaseJobStatus): Task[Unit] =
    actor ! ReleaseJob(peerId, job)

  def isDownloadCompleted: Task[Boolean] =
    actor ? IsDownloadCompleted

  def isRemoteInteresting(remoteHave: Set[PieceId]): Task[Boolean] =
    actor ? IsRemoteInteresting(remoteHave)

  def registerPeer(peerId: PeerId): Task[Unit] = ???

  def unregisterPeer(peerId: PeerId): Task[Unit] = ???

}

object DispatcherLive {
  def make: ZLayer[ActorSystem with DirectBufferPool with FileIO with Clock with Console, Throwable, Dispatcher] = {
    val effect = for {
      fileIO        <- ZIO.service[FileIO.Service]
      metaInfo      <- fileIO.metaInfo
      filesAreFresh <- fileIO.freshFilesWereAllocated

      localHave <- if (filesAreFresh)
                     ZIO.succeed(new Array[Boolean](metaInfo.pieceHashes.length))
                   else
                     for {
                       pieceRef    <- Ref.make[Int](0)
                       progressRef <- (updateProgress(pieceRef, metaInfo.pieceHashes.size) *>
                                        ZIO.sleep(1.second))
                                        .forever.fork
                       res         <- checkTorrent(metaInfo, pieceRef)
                       _           <- progressRef.interrupt.delay(1.second) *> putStrLn("")
                     } yield res

      res       <- make(Actor.State(metaInfo, localHave))

    } yield res

    effect.toLayer
  }

  private[dispatcher] def make(state: Actor.State): RIO[ActorSystem with Clock, DispatcherLive] = {
    for {
      system <- ZIO.service[ActorSystem.Service]
      actor  <- system.system.make(
                  "DispatcherLive",
                  actors.Supervisor.none,
                  state,
                  Actor.stateful
                )
    } yield DispatcherLive(actor)
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
      output   = SimpleProgressBar.render("Checking files", 60, progress)
      _       <- (putStr("\b" * output.length) *> putStr(output)).uninterruptible
    } yield ()
  }

}
