package torr.dispatcher

import torr.actorsystem.ActorSystem
import zio._
import zio.actors.ActorRef
import torr.dispatcher.Actor.{Command, ReleaseJob, TryAcquireJob}
import torr.metainfo.MetaInfo
import zio.clock.Clock

import scala.collection.mutable

case class DispatcherLive(private val actor: ActorRef[Command]) extends Dispatcher.Service {

  def tryAcquireJob(remoteHave: Set[PieceId]): Task[Option[DownloadJob]] = actor ? TryAcquireJob(remoteHave)

  def releaseJob(job: DownloadJob): Task[Unit] = actor ? ReleaseJob(job)
}

object DispatcherLive {
  def make(metaInfo: MetaInfo): ZLayer[ActorSystem with Clock, Throwable, Dispatcher] = {
    val effect = for {
      system <- ZIO.service[ActorSystem.Service]
      actor  <- system.system.make(
                  "DispatcherLive",
                  actors.Supervisor.none,
                  Actor.State(metaInfo, new mutable.HashSet[PieceId]()),
                  Actor.stateful
                )
    } yield DispatcherLive(actor)

    effect.toLayer
  }
}
