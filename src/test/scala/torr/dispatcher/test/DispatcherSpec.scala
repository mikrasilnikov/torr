package torr.dispatcher.test

import torr.dispatcher.{AcquireSuccess, Actor, DownloadJob, PieceId}
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSpec.toBytes
import zio._
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.util.Try

object DispatcherSpec extends DefaultRunnableSpec {
  def spec =
    suite("DispatcherSpec")(
      //
      test("allocates first job") {
        val state      = Actor.State(metaInfo, new mutable.HashSet[PieceId])
        val remoteHave = HashSet[PieceId](0, 1, 2)

        val expected = DownloadJob(0, metaInfo.pieceSize)
        val actual   = Actor.acquireJob(state, remoteHave).asInstanceOf[AcquireSuccess].job

        assert(actual)(equalTo(expected)) &&
        assert(state.activeJobs.size)(equalTo(1)) &&
        assert(state.activeJobs.head)(equalTo((0, expected)))
      },
      //
      test("allocates second job") {
        val state      = Actor.State(metaInfo, new mutable.HashSet[PieceId])
        val remoteHave = HashSet[PieceId](0, 1, 2)

        val expectedFirst  = DownloadJob(0, metaInfo.pieceSize)
        val expectedSecond = DownloadJob(1, metaInfo.pieceSize)
        val actualFirst    = Actor.acquireJob(state, remoteHave).asInstanceOf[AcquireSuccess].job
        val actualSecond   = Actor.acquireJob(state, remoteHave).asInstanceOf[AcquireSuccess].job

        assert(actualFirst)(equalTo(expectedFirst)) &&
        assert(actualSecond)(equalTo(expectedSecond))
      },
      //
      test("allocates suspended job") {
        val state = Actor.State(metaInfo, new mutable.HashSet[PieceId])

        val expected = DownloadJob(1, 100, 200)
        state.suspendedJobs.put(1, expected)

        val remoteHave = HashSet[PieceId](0, 1, 2)

        val actual = Actor.acquireJob(state, remoteHave).asInstanceOf[AcquireSuccess].job

        assert(actual)(equalTo(expected))
      },
      //
      test("allocates suitable job (using remote have)") {
        val state      = Actor.State(metaInfo, new mutable.HashSet[PieceId])
        val remoteHave = HashSet[PieceId](2)

        val expected = DownloadJob(2, metaInfo.pieceSize)
        val actual   = Actor.acquireJob(state, remoteHave).asInstanceOf[AcquireSuccess].job

        assert(actual)(equalTo(expected))
      },
      //
      test("releases inactive job") {
        val state = Actor.State(metaInfo, new mutable.HashSet[PieceId])

        val job    = DownloadJob(0, metaInfo.pieceSize)
        val actual = Try(Actor.releaseJob(state, job))

        assert(actual)(isFailure(hasMessage(equalTo("DownloadJob(0,262144,0) is not in state.activeJobs"))))
      },
      //
      test("releases completed job") {

        val mi = MetaInfo(
          announce = "udp://tracker.openbittorrent.com:80/announce",
          pieceSize = 262144,
          entries = FileEntry(Path("file1.dat"), 262144) :: Nil,
          pieceHashes = Vector(
            toBytes("da39a3ee5e6b4b0d3255bfef95601890afd80709")
          ),
          infoHash = Chunk.fromArray(
            Array[Byte](-81, -93, -38, -63, -123, 80, -128, -23, -44, 115, 25, 102, 115, 73, -8, -128, 1, -36, -23,
              -127)
          )
        )

        val state = Actor.State(mi, new mutable.HashSet[PieceId])

        val job = DownloadJob(0, mi.pieceSize, mi.pieceSize)
        state.activeJobs.put(0, job)

        Actor.releaseJob(state, job)

        assert(state.activeJobs.size)(equalTo(0)) &&
        assert(state.localHave(0))(isTrue)
      },
      //
      test("releases suspended job") {
        val state = Actor.State(metaInfo, new mutable.HashSet[PieceId])

        val job = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceSize / 2)
        state.activeJobs.put(0, job)
        Actor.releaseJob(state, job)

        assert(state.activeJobs.size)(equalTo(0)) &&
        assert(state.localHave(0))(isFalse) &&
        assert(state.suspendedJobs.size)(equalTo(1)) &&
        assert(state.suspendedJobs.head)(equalTo((0, job)))
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
