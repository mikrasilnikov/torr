package torr.dispatcher

import torr.actorsystem.ActorSystem
import torr.consoleui.SimpleProgressBar
import torr.directbuffers.DirectBufferPool
import zio._
import zio.actors.ActorRef
import torr.dispatcher.Actor.{Command, ReleaseJob, TryAcquireJob}
import torr.fileio.FileIO
import torr.metainfo.MetaInfo
import zio.clock.Clock
import zio.console.{Console, putStr, putStrLn}
import zio.duration.durationInt

import scala.collection.mutable

case class DispatcherLive(private val actor: ActorRef[Command]) extends Dispatcher.Service {

  def tryAcquireJob(remoteHave: Set[PieceId]): Task[Option[DownloadJob]] = actor ? TryAcquireJob(remoteHave)

  def releaseJob(job: DownloadJob): Task[Unit] = actor ? ReleaseJob(job)
}

object DispatcherLive {
  def make: ZLayer[ActorSystem with DirectBufferPool with FileIO with Clock with Console, Throwable, Dispatcher] = {
    val effect = for {
      system  <- ZIO.service[ActorSystem.Service]
      fileIO  <- ZIO.service[FileIO.Service]
      metaInfo = fileIO.metaInfo

      localHave <- if (fileIO.freshFilesWereAllocated) ZIO.succeed(new mutable.HashSet[PieceId]())
                   else
                     for {
                       pieceRef    <- Ref.make[Int](0)
                       progressRef <- (updateProgress(pieceRef, metaInfo.pieceHashes.size) *>
                                        ZIO.sleep(1.second))
                                        .forever.fork
                       res         <- checkTorrent(metaInfo, pieceRef)
                       _           <- progressRef.interrupt.delay(1.second) *> putStrLn("")
                     } yield res

      actor     <- system.system.make(
                     "DispatcherLive",
                     actors.Supervisor.none,
                     Actor.State(metaInfo, localHave),
                     Actor.stateful
                   )

    } yield DispatcherLive(actor)

    effect.toLayer
  }

  private def checkTorrent(
      metaInfo: MetaInfo,
      pieceRef: Ref[Int]
  ): RIO[FileIO with DirectBufferPool with Clock, mutable.Set[PieceId]] = {
    for {
      actualHashes <- ZIO.foreach(metaInfo.pieceHashes.indices.toVector) { pieceId =>
                        Hasher.hashPiece(metaInfo, pieceId) <* pieceRef.set(pieceId + 1)
                      }
      result        = new mutable.HashSet[PieceId]()
      _             = metaInfo.pieceHashes.indices.foreach { i =>
                        if (actualHashes(i) == metaInfo.pieceHashes(i))
                          result.add(i)
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
