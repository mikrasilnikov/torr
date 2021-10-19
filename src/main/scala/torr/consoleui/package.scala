package torr

import torr.dispatcher._
import torr.fileio._
import zio._
import zio.macros.accessible

package object consoleui {
  type ConsoleUI = Has[ConsoleUI.Service]

  @accessible
  object ConsoleUI {
    trait Service {
      def clear: Task[Unit]
      def draw: RIO[Dispatcher with FileIO, Unit]
    }
  }
}
