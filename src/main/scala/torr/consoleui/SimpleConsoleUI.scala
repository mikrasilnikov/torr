package torr.consoleui

import torr.dispatcher.Actor.{State => DispatcherState}
import zio._
import zio.console._

case class SimpleConsoleUI(private val console: Console.Service, private var lastRender: String = "")
    extends ConsoleUI.Service {

  private val megabyte = 1024 * 1024
  private val kilobyte = 1024

  def clear: Task[Unit] = console.putStr("\b" * lastRender.length)

  def draw(state: DispatcherState): Task[Unit] = {

    val result = new StringBuilder

    val progressBar = TorrentProgressBar.renderProgressBar(state, "Downloading     ", 50)
    result.append(progressBar)
    result.append(" ")

    val dlSpeed = state.registeredPeers.map { case (_, peer) => peer.downloadSpeed }.sum
    val ulSpeed = state.registeredPeers.map { case (_, peer) => peer.uploadSpeed }.sum

    if (dlSpeed > megabyte) {
      val spd = dlSpeed.toDouble / megabyte
      result.append("%.1f".format(spd))
      result.append(" MB/s")
    } else if (dlSpeed > kilobyte) {
      val spd = dlSpeed.toDouble / kilobyte
      result.append("%.0f".format(spd))
      result.append(" KB/s")
    } else {
      val spd = dlSpeed.toDouble / kilobyte
      result.append("%.0f".format(spd))
      result.append(" B/s")
    }

    result.append(" down ")

    if (ulSpeed > megabyte) {
      val spd = ulSpeed.toDouble / megabyte
      result.append("%.1f".format(spd))
      result.append(" MB/s")
    } else if (ulSpeed > kilobyte) {
      val spd = ulSpeed.toDouble / kilobyte
      result.append("%.0f".format(spd))
      result.append(" KB/s")
    } else {
      val spd = ulSpeed.toDouble / kilobyte
      result.append("%.0f".format(spd))
      result.append(" B/s")
    }

    result.append(" up")

    lastRender = result.result()

    console.putStr(lastRender)
  }
}

object SimpleConsoleUI {
  def make: ZLayer[Console, Nothing, ConsoleUI] = {

    val effect = for {
      console <- ZIO.service[Console.Service]
    } yield SimpleConsoleUI(console)

    effect.toLayer
  }
}
