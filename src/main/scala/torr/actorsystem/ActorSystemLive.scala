package torr.actorsystem

import zio._
import zio.logging.Logging

case class ActorSystemLive(private val actorSystem: actors.ActorSystem) extends ActorSystem.Service {
  val system: actors.ActorSystem = actorSystem
}

object ActorSystemLive {

  def make(name: String): ZLayer[Logging, Throwable, ActorSystem] = {

    val managed: ZManaged[Logging, Throwable, ActorSystem.Service] = for {
      _      <- Logging.debug("Creating actor system").toManaged_
      system <- actors.ActorSystem(name)
                  .toManaged(sys =>
                    Logging.debug("Shutting down actor system") *>
                      sys.shutdown.ignore *>
                      Logging.debug("Actor system has been shut down successfully")
                  )
    } yield ActorSystemLive(system)

    ZLayer.fromManaged(managed)
  }
}
