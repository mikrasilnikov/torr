package torr.examples

import torr.consoleui.RichConsoleUI._

object RichUITest {
  def main(args: Array[String]) = {

    val vm = ViewModel(
      localHave = new Array[Boolean](100),
      dlSpeed = 5 * 1024 * 1024,
      ulSpeed = 5 * 1024 * 1024,
      connectionsNum = 100,
      maxConnections = 500,
      activePeers =
        ActivePeerViewModel("255.255.255.255:12345", 5 * 1024 * 1024, 5 * 1024 * 1024) ::
          ActivePeerViewModel("255.255.255.255:12345", 5 * 1024 * 1024, 5 * 1024 * 1024) :: Nil
    )

    val res = render(vm)

    println(res)

  }
}
