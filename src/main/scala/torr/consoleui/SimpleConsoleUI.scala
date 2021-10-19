package torr.consoleui

import torr.consoleui.SimpleConsoleUI.ViewModel
import torr.dispatcher.Actor.{State => DispatcherState}
import torr.dispatcher.Dispatcher
import torr.fileio.FileIO
import zio._
import zio.console._
import torr.consoleui.StringExtensions._
import scala.language.implicitConversions

case class SimpleConsoleUI(private val console: Console.Service, private var lastRender: String = "")
    extends ConsoleUI.Service {

  private val megabyte = 1024 * 1024
  private val kilobyte = 1024

  def clear: Task[Unit] = console.putStr("\b" * lastRender.length)

  def draw: RIO[Dispatcher with FileIO, Unit] = {
    for {
      viewModel <- Dispatcher.mapState(viewModelFromState)
      result     = render(viewModel)
      _         <- console.putStr(result)
      _          = lastRender = result
    } yield ()
  }

  private def viewModelFromState(state: DispatcherState): ViewModel = {
    ViewModel(
      state.localHave,
      state.registeredPeers.map { case (_, peer) => peer.downloadSpeed }.sum,
      state.registeredPeers.map { case (_, peer) => peer.uploadSpeed }.sum
    )
  }

  private def render(viewModel: ViewModel): String = {
    val sb = new StringBuilder

    val message = viewModel.localHave.forall(identity) match {
      case false => "Downloading".padRight(16)
      case true  => "Seeding".padRight(16)
    }

    val progressBar = TorrentProgressBar.renderProgressBar(viewModel.localHave, message, 50)
    sb.append(progressBar)
    sb.append(" ")

    if (viewModel.dlSpeed > megabyte) {
      val spd = viewModel.dlSpeed.toDouble / megabyte
      sb.append("%.1f".format(spd).padLeft(4))
      sb.append(" MB/s")
    } else if (viewModel.dlSpeed > kilobyte) {
      val spd = viewModel.dlSpeed.toDouble / kilobyte
      sb.append("%.0f".format(spd).padLeft(4))
      sb.append(" KB/s")
    } else {
      val spd = viewModel.dlSpeed.toDouble
      sb.append("%.0f".format(spd).padLeft(4))
      sb.append("  B/s")
    }

    sb.append(" down, ")

    if (viewModel.ulSpeed > megabyte) {
      val spd = viewModel.ulSpeed.toDouble / megabyte
      sb.append("%.1f".format(spd).padLeft(4))
      sb.append(" MB/s")
    } else if (viewModel.ulSpeed > kilobyte) {
      val spd = viewModel.ulSpeed.toDouble / kilobyte
      sb.append("%.0f".format(spd).padLeft(4))
      sb.append(" KB/s")
    } else {
      val spd = viewModel.ulSpeed.toDouble
      sb.append("%.0f".format(spd).padLeft(4))
      sb.append("  B/s")
    }

    sb.append(" up")

    sb.result()
  }
}

object SimpleConsoleUI {

  final case class ViewModel(localHave: Array[Boolean], dlSpeed: Int, ulSpeed: Int)

  def make: ZLayer[Console, Nothing, ConsoleUI] = {

    val effect = for {
      console <- ZIO.service[Console.Service]
    } yield SimpleConsoleUI(console)

    effect.toLayer
  }
}
