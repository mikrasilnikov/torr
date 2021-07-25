package torr

import zio._
import zio.actors.Actor.Stateful
import zio.actors.{ActorSystem, Context}
import zio.console.putStrLn
import zio.logging.slf4j.Slf4jLogger

object Main extends App {

  sealed trait Cmd[+_]
  final case class Add(a: Int, B: Int) extends Cmd[Int]
  final case class Mul(a: Int, B: Int) extends Cmd[Int]

  val stateful = new Stateful[Any, Unit, Cmd] {
    override def receive[A](state: Unit, msg: Cmd[A], context: Context): Task[(Unit, A)] = {
      msg match {
        case Add(a, b) => ZIO((), a + b)
        case Mul(a, b) => ZIO((), a * b)
      }
    }
  }

  val effect = for {
    system <- ActorSystem("Test system")
    actor  <- system.make("math1", actors.Supervisor.none, (), stateful)
    res1   <- actor ? Add(1, 2)
    res2   <- actor ? Mul(2, 3)
    _      <- putStrLn(s"res1 = $res1, res2 = $res2")
  } yield ()

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] = {
    val logging = Slf4jLogger.make((_, message) => message)
    effect.provideCustomLayer(logging).exitCode
  }
}
