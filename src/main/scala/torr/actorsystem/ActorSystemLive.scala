package torr.actorsystem

import zio._

case class ActorSystemLive(private val actorSystem: actors.ActorSystem) extends ActorSystem.Service {
  val system: actors.ActorSystem = actorSystem
}

object ActorSystemLive {

  def make(name: String): ZLayer[Any, Throwable, ActorSystem] = {

    val managed: ZManaged[Any, Throwable, ActorSystem.Service] = for {
      system <- actors.ActorSystem(name).toManaged(_.shutdown.ignore)
    } yield ActorSystemLive(system)

    ZLayer.fromManaged(managed)
  }
}
