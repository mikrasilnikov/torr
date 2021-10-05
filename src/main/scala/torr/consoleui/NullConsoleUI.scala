package torr.consoleui

import torr.dispatcher.Actor
import zio._
import zio.console.Console

case class NullConsoleUI() extends ConsoleUI.Service {
  def clear: Task[Unit]                    = ZIO.unit
  def draw(state: Actor.State): Task[Unit] = ZIO.unit
}

object NullConsoleUI {
  def make: ZLayer[Any, Nothing, ConsoleUI] = ZLayer.succeed(NullConsoleUI())
}
