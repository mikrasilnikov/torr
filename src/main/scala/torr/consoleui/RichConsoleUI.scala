package torr.consoleui
import org.fusesource.jansi.Ansi.ansi
import org.fusesource.jansi.AnsiConsole
import torr.consoleui.StringExtensions.StringOps
import torr.dispatcher.Dispatcher
import torr.fileio.FileIO
import zio.console.Console
import zio._
import torr.dispatcher.Actor.{State => DispatcherState}

case class RichConsoleUI(
    private val console: Console.Service,
    private val maxConnections: Int,
    private var lastRender: Option[String] = None
) extends ConsoleUI.Service {

  def clear: Task[Unit] = ZIO.unit

  def draw: RIO[Dispatcher with FileIO, Unit] = {
    for {
      viewModel <- Dispatcher.mapState(state => RichConsoleUI.stateToViewModel(state, maxConnections))
      result     = RichConsoleUI.render(viewModel)
      _         <- console.putStr(ansi().eraseScreen().toString).when(
                     lastRender.isEmpty || result.length != lastRender.get.length
                   )
      _         <- console.putStr(ansi().cursor(0, 0).toString)
      _         <- console.putStr(result)
      _          = lastRender = Some(result)
    } yield ()
  }

}

object RichConsoleUI {

  private val megabyte = 1024 * 1024
  private val kilobyte = 1024

  def make(maxConnections: Int): ZLayer[Console, Throwable, ConsoleUI] = {

    val effect = for {
      _       <- ZIO(AnsiConsole.systemInstall())
      console <- ZIO.service[Console.Service]
    } yield RichConsoleUI(console, maxConnections)

    effect
      .toManaged(_ => ZIO(AnsiConsole.systemUninstall()).orDie)
      .toLayer
  }

  final case class ActivePeerViewModel(address: String, dlSpeed: Int, ulSpeed: Int)

  final case class ViewModel(
      localHave: Array[Boolean],
      dlSpeed: Int,
      ulSpeed: Int,
      connectionsNum: Int,
      maxConnections: Int,
      activePeers: List[ActivePeerViewModel]
  )

  def stateToViewModel(state: DispatcherState, maxConnections: Int): ViewModel = {
    ViewModel(
      state.localHave,
      dlSpeed = state.registeredPeers.map { case (_, peer) => peer.downloadSpeed }.sum,
      ulSpeed = state.registeredPeers.map { case (_, peer) => peer.uploadSpeed }.sum,
      connectionsNum = state.registeredPeers.size,
      maxConnections,
      activePeers = state.registeredPeers
        .filter { case (_, peer) => peer.downloadSpeed > 0 || peer.uploadSpeed > 0 }
        .map {
          case (_, peer) =>
            ActivePeerViewModel(
              address = peer.address.toString().dropWhile(_ != '/').drop(1),
              dlSpeed = peer.downloadSpeed,
              ulSpeed = peer.uploadSpeed
            )
        }.toList
    )
  }

  def render(viewModel: ViewModel): String = {
    val sb = new StringBuilder

    sb.append("\n")

    val message = viewModel.localHave.forall(identity) match {
      case false => "Downloading".padRight(15)
      case true  => "Seeding".padRight(15)
    }

    val progressBar = TorrentProgressBar.renderProgressBar(viewModel.localHave, message, 50)
    sb.append(progressBar)

    sb.append("\n")
    sb.append(ansi().eraseLine())

    sb.append(" " * 18)
    sb.append("Connections ")
    sb.append(viewModel.connectionsNum.toString.padLeft(3))
    sb.append(" / ")
    sb.append(viewModel.maxConnections.toString.padLeft(3))

    sb.append("\n")
    sb.append(ansi().eraseLine().toString)
    sb.append("\n")
    sb.append(ansi().eraseLine().toString)

    if (viewModel.activePeers.nonEmpty) {
      sb.append(" " * 18)
      sb.append("Address".padLeft(13).padRight(21))
      sb.append(" | ")
      sb.append("Down".padLeft(7).padRight(10))
      sb.append(" | ")
      sb.append("Up".padLeft(6).padRight(10))
      sb.append("\n")
      sb.append(ansi().eraseLine().toString)
      sb.append("\n")
      sb.append(ansi().eraseLine().toString)

      viewModel.activePeers.foreach { peerVM =>
        sb.append(" " * 18)
        sb.append(peerVM.address.padLeft(21))
        sb.append(" | ")
        sb.append(renderSpeed(peerVM.dlSpeed))
        sb.append("  | ")
        sb.append(renderSpeed(peerVM.ulSpeed))
        sb.append("\n")
      }

      sb.append("\n")
      sb.append(ansi().eraseLine().toString)
      sb.append(" " * 18)
      sb.append("Total".padLeft(12).padRight(21))
      sb.append(" | ")
      sb.append(renderSpeed(viewModel.dlSpeed))
      sb.append("  | ")
      sb.append(renderSpeed(viewModel.ulSpeed))
      sb.append("\n")
    }
    sb.result()
  }

  def renderSpeed(speed: Int): StringBuilder = {
    val sb = new StringBuilder
    if (speed > megabyte) {
      val spd = speed.toDouble / megabyte
      sb.append("%.1f".format(spd).padLeft(4))
      sb.append(" MB/s")
    } else if (speed > kilobyte) {
      val spd = speed.toDouble / kilobyte
      sb.append("%.0f".format(spd).padLeft(4))
      sb.append(" KB/s")
    } else {
      val spd = speed.toDouble
      sb.append("%.0f".format(spd).padLeft(4))
      sb.append("  B/s")
    }
    sb
  }

}
