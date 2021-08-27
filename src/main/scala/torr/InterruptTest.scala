package torr

import zio._
import zio.actors.Actor.Stateful
import zio.actors.{ActorRef, ActorSystem, Context}
import zio.clock.Clock
import zio.console.{Console, putStrLn}
import zio.duration.durationInt

import java.io.IOException

case class CacheEntry(data: String) {
  var writeOutFiber: Option[Fiber[Throwable, Unit]] = None
}

sealed trait Command[+_]
case class TestCommand(value: CacheEntry) extends Command[Unit]

object InterruptTest extends App {

  val stateful = new Stateful[Console, Unit, Command] {
    override def receive[A](state: Unit, msg: Command[A], context: Context) =
      msg match {
        case TestCommand(value) => putStrLn(s"Received $value").as((), ())
      }
  }

  def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {

    val entry  = CacheEntry("Hello")
    val effect = for {
      system <- ActorSystem("System")
      actor  <- system.make("actor1", actors.Supervisor.none, (), stateful)
      _      <- scheduleWriteOut(actor, entry)
      _      <- scheduleWriteOut(actor, entry)
      _      <- scheduleWriteOut(actor, entry)
      _      <- putStrLn("!")
      _      <- entry.writeOutFiber.get.join
    } yield ()

    effect.exitCode
  }

  private def scheduleWriteOut(
      actor: ActorRef[Command],
      entry: CacheEntry
  ): ZIO[Console with Clock, Nothing, Unit] =
    for {
      _     <- entry.writeOutFiber match {
                 case Some(fiber) => fiber.interrupt
                 case None        => ZIO.unit
               }
      fiber <- (actor ! TestCommand(entry)).delay(5.seconds).fork
      _      = entry.writeOutFiber = Some(fiber)
    } yield ()

}
