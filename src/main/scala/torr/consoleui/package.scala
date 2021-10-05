package torr

import zio._
import zio.macros.accessible
import torr.dispatcher.Actor.{State => DispatcherState}
import zio.console.Console

package object consoleui {
  type ConsoleUI = Has[ConsoleUI.Service]

  @accessible
  object ConsoleUI {
    trait Service {
      def clear: Task[Unit]
      def draw(state: DispatcherState): Task[Unit]
    }
  }
}
