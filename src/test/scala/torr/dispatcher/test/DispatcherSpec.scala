package torr.dispatcher.test

import torr.dispatcher.{Actor, DownloadJob, PieceId}
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSpec.toBytes
import zio._
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec

import scala.collection.immutable.HashSet
import scala.collection.mutable

object DispatcherSpec extends DefaultRunnableSpec {
  def spec =
    suite("DispatcherSpec")(
      //
      test("allocates first job") {
        val state      = Actor.State(metaInfo, new mutable.HashSet[PieceId])
        val remoteHave = HashSet[PieceId](0, 1, 2)

        val expected = DownloadJob(0, 0, metaInfo.pieceSize)
        val actual   = Actor.tryAllocateJob(state, remoteHave).get

        assert(expected)(equalTo(actual))
      },
      //
      test("allocates second job") {
        val state      = Actor.State(metaInfo, new mutable.HashSet[PieceId])
        val remoteHave = HashSet[PieceId](0, 1, 2)

        val expectedFirst  = DownloadJob(0, 0, metaInfo.pieceSize)
        val expectedSecond = DownloadJob(1, 0, metaInfo.pieceSize)
        val actualFirst    = Actor.tryAllocateJob(state, remoteHave).get
        val actualSecond   = Actor.tryAllocateJob(state, remoteHave).get

        assert(expectedFirst)(equalTo(actualFirst)) &&
        assert(expectedSecond)(equalTo(actualSecond))
      }
    )

  private val metaInfo = MetaInfo(
    announce = "udp://tracker.openbittorrent.com:80/announce",
    pieceSize = 262144,
    entries = FileEntry(Path("file1.dat"), 524288) :: Nil,
    pieceHashes = Vector(
      toBytes("2e000fa7e85759c7f4c254d4d9c33ef481e459a7"),
      toBytes("d93b208338769447004e90bf142769fc004d8b0c")
    ),
    infoHash = Chunk.fromArray(
      Array[Byte](-81, -93, -38, -63, -123, 80, -128, -23, -44, 115, 25, 102, 115, 73, -8, -128, 1, -36, -23, -127)
    )
  )
}
