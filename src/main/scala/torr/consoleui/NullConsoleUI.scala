package torr.consoleui

import torr.dispatcher.{Actor, Dispatcher}
import torr.fileio.FileIO
import zio._
import zio.console.Console

case class NullConsoleUI() extends ConsoleUI.Service {
  def clear: Task[Unit]                       = ZIO.unit
  def draw: RIO[Dispatcher with FileIO, Unit] = ZIO.unit
}

object NullConsoleUI {
  def make: ZLayer[Any, Nothing, ConsoleUI] = ZLayer.succeed(NullConsoleUI())
}
