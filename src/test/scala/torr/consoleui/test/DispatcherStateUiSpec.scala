package torr.consoleui.test

import torr.consoleui.DispatcherStateUi
import zio._
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._
import torr.dispatcher.Actor.{State => DispatcherState}
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSpec.toBytes

import scala.collection.mutable

object DispatcherStateUiSpec extends DefaultRunnableSpec {
  override def spec =
    suite("DispatcherStateUiSpec")(
      //
      test("ProgressBar - empty (10 pcs)") {
        val expected = "Downloading [          ]   0%"
        val state    = DispatcherState(metaInfo(10), new Array[Boolean](10))
        val actual   = DispatcherStateUi.renderProgressBar(state, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - empty (1 pcs)") {
        val expected = "Downloading [          ]   0%"
        val state    = DispatcherState(metaInfo(1), new Array[Boolean](1))
        val actual   = DispatcherStateUi.renderProgressBar(state, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - full (10 pcs)") {
        val expected = "Downloading [==========] 100%"
        val state    = DispatcherState(metaInfo(10), new Array[Boolean](10))
        (0 until 10).foreach(state.localHave(_) = true)
        val actual   = DispatcherStateUi.renderProgressBar(state, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - full (1 pcs)") {
        val expected = "Downloading [==========] 100%"
        val state    = DispatcherState(metaInfo(1), new Array[Boolean](1))
        (0 until 1).foreach(state.localHave(_) = true)
        val actual   = DispatcherStateUi.renderProgressBar(state, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - left 50% (10 pcs)") {
        val expected = "Downloading [=====     ]  50%"
        val state    = DispatcherState(metaInfo(10), new Array[Boolean](10))
        (0 to 4).foreach(state.localHave(_) = true)
        val actual   = DispatcherStateUi.renderProgressBar(state, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - right 50% (10 pcs)") {
        val expected = "Downloading [     =====]  50%"
        val state    = DispatcherState(metaInfo(10), new Array[Boolean](10))
        (5 to 9).foreach(state.localHave(_) = true)
        val actual   = DispatcherStateUi.renderProgressBar(state, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - center 10% (10 pcs)") {
        val expected = "Downloading [    =     ]  10%"
        val state    = DispatcherState(metaInfo(10), new Array[Boolean](10))
        (4 to 4).foreach(state.localHave(_) = true)
        val actual   = DispatcherStateUi.renderProgressBar(state, "Downloading", 10)
        assert(actual)(equalTo(expected))
      },
      //
      test("ProgressBar - center 5% (10 pcs)") {
        val expected = "Downloading [          ]   5%"
        val state    = DispatcherState(metaInfo(20), new Array[Boolean](20))
        (10 to 10).foreach(state.localHave(_) = true)
        val actual   = DispatcherStateUi.renderProgressBar(state, "Downloading", 10)
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
