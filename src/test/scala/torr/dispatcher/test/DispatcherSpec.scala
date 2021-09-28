package torr.dispatcher.test

import torr.dispatcher
import torr.dispatcher.{AcquireSuccess, Actor, DownloadJob, PeerId, PieceId}
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
      test("registers peer") {
        val state  = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val peerId = Chunk.fill[Byte](20)(1)
        Actor.registerPeer(state, peerId)

        val expected = new mutable.HashSet[PeerId]()
        expected.add(peerId)

        assert(state.registeredPeers)(equalTo(expected))
      },
      //
      test("registers peer - fails if peer is already registered") {
        val state  = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val peerId = Chunk.fill[Byte](20)(100)
        Actor.registerPeer(state, peerId)
        val actual = Try { Actor.registerPeer(state, peerId) }
        assert(actual)(isFailure(hasMessage(equalTo("PeerId dddddddddddddddddddd is already registered"))))
      },
      //
      test("unregisters peer - removes allocated job") {
        val peerId     = Chunk.fill[Byte](20)(100)
        val registered = new mutable.HashSet[PeerId]()
        registered.add(peerId)

        val jobs = new mutable.HashMap[(PieceId, PeerId), DownloadJob]()
        jobs.put((0, peerId), DownloadJob(0, metaInfo.pieceSize))

        val state =
          Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces), registeredPeers = registered, activeJobs = jobs)

        Actor.unregisterPeer(state, peerId)

        assert(state.registeredPeers)(equalTo(new mutable.HashSet[PeerId])) &&
        assert(state.activeJobs)(equalTo(new mutable.HashMap[(PieceId, PeerId), DownloadJob]))
      },
      //
      test("unregisters peer - fails if peer is not registered") {
        val peerId = Chunk.fill[Byte](20)(100)

        val registered = new mutable.HashSet[PeerId]()
        registered.add(peerId)

        val state = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces), registeredPeers = registered)

        Actor.unregisterPeer(state, peerId)
        val actual = Try { Actor.unregisterPeer(state, peerId) }

        assert(actual)(isFailure(hasMessage(equalTo("PeerId dddddddddddddddddddd is not registered"))))
      },
      //
      test("allocates first job") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](0, 1, 2)

        val peerId      = Chunk.fill[Byte](20)(0)
        val expectedJob = DownloadJob(0, metaInfo.pieceSize)
        val actualJob   = Actor.acquireJob(state, peerId, remoteHave).asInstanceOf[AcquireSuccess].job

        assert(actualJob)(equalTo(expectedJob)) &&
        assert(state.activeJobs.size)(equalTo(1)) &&
        assert(state.activeJobs.head)(equalTo((0, peerId), expectedJob))
      },
      //
      test("allocates second job") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](0, 1, 2)

        val peerId         = Chunk.fill[Byte](20)(0)
        val expectedFirst  = DownloadJob(0, metaInfo.pieceSize)
        val expectedSecond = DownloadJob(1, metaInfo.pieceSize)
        val actualFirst    = Actor.acquireJob(state, peerId, remoteHave).asInstanceOf[AcquireSuccess].job
        val actualSecond   = Actor.acquireJob(state, peerId, remoteHave).asInstanceOf[AcquireSuccess].job

        assert(actualFirst)(equalTo(expectedFirst)) &&
        assert(actualSecond)(equalTo(expectedSecond))
      },
      //
      test("allocates suspended job") {
        val state = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))

        val peerId   = Chunk.fill[Byte](20)(0)
        val expected = DownloadJob(1, 100, 200)
        state.suspendedJobs.put(1, expected)

        val remoteHave = HashSet[PieceId](0, 1, 2)

        val actualJob = Actor.acquireJob(state, peerId, remoteHave).asInstanceOf[AcquireSuccess].job

        assert(actualJob)(equalTo(expected))
      },
      //
      test("allocates suitable job (using remote have)") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](1)

        val peerId   = Chunk.fill[Byte](20)(0)
        val expected = DownloadJob(1, metaInfo.pieceSize)
        val actual   = Actor.acquireJob(state, peerId, remoteHave).asInstanceOf[AcquireSuccess].job

        assert(actual)(equalTo(expected))
      },
      //
      test("allocates job - not interested") {
        val localHave  = new Array[Boolean](metaInfo.numPieces)
        localHave(0) = true
        val state      = Actor.State(metaInfo, localHave)
        val remoteHave = HashSet[PieceId](0)

        val peerId = Chunk.fill[Byte](20)(0)
        val actual = Actor.acquireJob(state, peerId, remoteHave).asInstanceOf[dispatcher.NotInterested.type]

        assert(actual)(equalTo(dispatcher.NotInterested))
      },
      //
      test("allocates job - download completed") {
        val localHave  = new Array[Boolean](metaInfo.numPieces)
        localHave(0) = true
        localHave(1) = true
        val state      = Actor.State(metaInfo, localHave)
        val remoteHave = HashSet[PieceId](0)
        val peerId     = Chunk.fill[Byte](20)(0)
        val actual     = Actor.acquireJob(state, peerId, remoteHave).asInstanceOf[dispatcher.DownloadCompleted.type]

        assert(actual)(equalTo(dispatcher.DownloadCompleted))
      },
      //
      test("releases inactive job") {
        val state = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))

        val peerId = Chunk.fill[Byte](20)(0)
        val job    = DownloadJob(0, metaInfo.pieceSize)
        val actual = Try(Actor.releaseJob(state, peerId, job))

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

        val peerId = Chunk.fill[Byte](20)(0)

        val state = Actor.State(mi, new Array[Boolean](metaInfo.numPieces))

        val job = DownloadJob(0, mi.pieceSize, mi.pieceSize)
        state.activeJobs.put((0, peerId), job)

        Actor.releaseJob(state, peerId, job)

        assert(state.activeJobs.size)(equalTo(0)) &&
        assert(state.localHave(0))(isTrue)
      },
      //
      test("releases suspended job") {
        val state = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))

        val peerId = Chunk.fill[Byte](20)(0)

        val job = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceSize / 2)
        state.activeJobs.put((0, peerId), job)
        Actor.releaseJob(state, peerId, job)

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
