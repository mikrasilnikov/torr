package torr.dispatcher

import torr.peerwire.TorrBitSet
import zio._
import zio.actors.Actor.Stateful
import zio.actors.Context

import scala.collection.mutable

object Actor {
  sealed trait Command[+_]
  case class TryAllocateJob(remoteHave: TorrBitSet) extends Command[Option[DownloadJob]]
  case class ReleaseJob(job: DownloadJob)           extends Command[Unit]

  case class State(
      localHave: TorrBitSet
  )

  val stateful = new Stateful[Any, State, Command] {
    def receive[A](state: State, msg: Command[A], context: Context): RIO[Any, (State, A)] =
      msg match {
        case TryAllocateJob(have) => tryAllocateJob(have).map(res => (state, res))
        case _                    => ???
      }
  }

  private[dispatcher] def tryAllocateJob(remoteHave: TorrBitSet): Task[Option[DownloadJob]] = {
    ???
  }

}
