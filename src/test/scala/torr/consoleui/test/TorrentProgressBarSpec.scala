package torr.consoleui.test

import torr.consoleui.TorrentProgressBar
import zio._
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._
import torr.dispatcher.Actor.{State => DispatcherState}
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSpec.toBytes

import scala.collection.mutable

object TorrentProgressBarSpec extends DefaultRunnableSpec {
  override def spec =
    suite("DispatcherStateUiSpec")(
      //
      test("ProgressBar - empty (10 pcs)") {
        val expected = "Downloading [          ]   0%"
        val actual   = TorrentProgressBar.renderProgressBar(new Array[Boolean](10), "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - empty (1 pcs)") {
        val expected = "Downloading [          ]   0%"
        val actual   = TorrentProgressBar.renderProgressBar(new Array[Boolean](1), "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - full (10 pcs)") {
        val expected  = "Downloading [==========] 100%"
        val localHave = new Array[Boolean](10)
        (0 until 10).foreach(localHave(_) = true)
        val actual    = TorrentProgressBar.renderProgressBar(localHave, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - full (1 pcs)") {
        val expected  = "Downloading [==========] 100%"
        val localHave = new Array[Boolean](1)
        (0 until 1).foreach(localHave(_) = true)
        val actual    = TorrentProgressBar.renderProgressBar(localHave, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - left 50% (10 pcs)") {
        val expected  = "Downloading [=====     ]  50%"
        val localHave = new Array[Boolean](10)
        (0 to 4).foreach(localHave(_) = true)
        val actual    = TorrentProgressBar.renderProgressBar(localHave, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - right 50% (10 pcs)") {
        val expected  = "Downloading [     =====]  50%"
        val localHave = new Array[Boolean](10)
        (5 to 9).foreach(localHave(_) = true)
        val actual    = TorrentProgressBar.renderProgressBar(localHave, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - center 10% (10 pcs)") {
        val expected  = "Downloading [    =     ]  10%"
        val localHave = new Array[Boolean](10)
        (4 to 4).foreach(localHave(_) = true)
        val actual    = TorrentProgressBar.renderProgressBar(localHave, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - center 5% (10 pcs)") {
        val expected  = "Downloading [     -    ]   5%"
        val localHave = new Array[Boolean](20)
        (10 to 10).foreach(localHave(_) = true)
        val actual    = TorrentProgressBar.renderProgressBar(localHave, "Downloading", 10)
        assert(actual)(equalTo(expected))
      }
    )

  private def metaInfo(numPieces: Int) =
    MetaInfo(
      announce = "udp://tracker.openbittorrent.com:80/announce",
      pieceSize = 262144,
      entries = FileEntry(Path("file1.dat"), 524288) :: Nil,
      pieceHashes = (1 to numPieces).map(_ => toBytes("2e000fa7e85759c7f4c254d4d9c33ef481e459a7")).toVector,
      infoHash = Chunk.fromArray(
        Array[Byte](-81, -93, -38, -63, -123, 80, -128, -23, -44, 115, 25, 102, 115, 73, -8, -128, 1, -36, -23, -127)
      )
    )
}
