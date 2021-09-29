package torr.dispatcher.test

import torr.dispatcher
import torr.dispatcher.Actor.AcquireJobRequest
import torr.dispatcher.{AcquireJobResult, Actor, DownloadJob, PeerId, PieceId, ReleaseJobStatus}
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSpec.toBytes
import zio._
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
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

        val job         = DownloadJob(0, metaInfo.pieceSize, Chunk.fill(20)(123.toByte))
        val activeJobs  = new mutable.HashMap[DownloadJob, PeerId]()
        activeJobs.put(job, peerId)
        val activePeers = new mutable.HashMap[PeerId, ArrayBuffer[DownloadJob]]
        activePeers.put(peerId, ArrayBuffer(job))

        val state =
          Actor.State(
            metaInfo,
            new Array[Boolean](metaInfo.numPieces),
            registeredPeers = registered,
            activeJobs = activeJobs,
            activePeers = activePeers
          )

        Actor.unregisterPeer(state, peerId)

        assert(state.registeredPeers)(equalTo(new mutable.HashSet[PeerId])) &&
        assert(state.activeJobs)(equalTo(new mutable.HashMap[DownloadJob, PeerId]))
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
      testM("allocates first job") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](0, 1, 2)

        val peerId   = Chunk.fill[Byte](20)(0)
        val expected = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0))

        Actor.registerPeer(state, peerId)
        for {
          p      <- Promise.make[Throwable, AcquireJobResult]
          _      <- Actor.acquireJob(state, peerId, remoteHave, p)
          actual <- p.await.map(_.asInstanceOf[AcquireJobResult.AcquireSuccess].job)
        } yield //
        assert(actual)(equalTo(expected)) &&
          assert(state.activeJobs.size)(equalTo(1)) &&
          assert(state.activeJobs.head)(equalTo(expected, peerId))
      },
      //
      testM("allocates second job") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](0, 1, 2)

        val peerId         = Chunk.fill[Byte](20)(0)
        val expectedFirst  = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0))
        val expectedSecond = DownloadJob(1, metaInfo.pieceSize, metaInfo.pieceHashes(1))

        Actor.registerPeer(state, peerId)
        for {
          p1      <- Promise.make[Throwable, AcquireJobResult]
          p2      <- Promise.make[Throwable, AcquireJobResult]
          _       <- Actor.acquireJob(state, peerId, remoteHave, p1)
          _       <- Actor.acquireJob(state, peerId, remoteHave, p2)
          actual1 <- p1.await.map(_.asInstanceOf[AcquireJobResult.AcquireSuccess].job)
          actual2 <- p2.await.map(_.asInstanceOf[AcquireJobResult.AcquireSuccess].job)
        } yield //
        assert(actual1)(equalTo(expectedFirst)) &&
          assert(actual2)(equalTo(expectedSecond))
      },
      //
      testM("allocates suspended job") {
        val state = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))

        val peerId   = Chunk.fill[Byte](20)(0)
        val expected = DownloadJob(1, 100, metaInfo.pieceHashes(1), 200)
        state.suspendedJobs.put(1, expected)

        val remoteHave = HashSet[PieceId](0, 1, 2)

        Actor.registerPeer(state, peerId)
        for {
          p      <- Promise.make[Throwable, AcquireJobResult]
          _      <- Actor.acquireJob(state, peerId, remoteHave, p)
          actual <- p.await.map(_.asInstanceOf[AcquireJobResult.AcquireSuccess].job)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("allocates suitable job (using remote have)") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](1)

        val peerId   = Chunk.fill[Byte](20)(0)
        val expected = DownloadJob(1, metaInfo.pieceSize, metaInfo.pieceHashes(1))

        Actor.registerPeer(state, peerId)
        for {
          p      <- Promise.make[Throwable, AcquireJobResult]
          _      <- Actor.acquireJob(state, peerId, remoteHave, p)
          actual <- p.await.map(_.asInstanceOf[AcquireJobResult.AcquireSuccess].job)
        } yield assert(actual)(equalTo(expected))

      },
      //
      testM("allocates job - not interested") {
        val localHave  = new Array[Boolean](metaInfo.numPieces)
        localHave(0) = true
        val state      = Actor.State(metaInfo, localHave)
        val remoteHave = HashSet[PieceId](0)
        val peerId     = Chunk.fill[Byte](20)(0)

        Actor.registerPeer(state, peerId)
        for {
          p      <- Promise.make[Throwable, AcquireJobResult]
          _      <- Actor.acquireJob(state, peerId, remoteHave, p)
          actual <- p.await.map(_.asInstanceOf[AcquireJobResult.NotInterested.type])
        } yield assert(actual)(equalTo(AcquireJobResult.NotInterested))
      },
      //
      testM("allocates job - download completed") {
        val localHave  = new Array[Boolean](metaInfo.numPieces)
        localHave(0) = true
        localHave(1) = true
        val state      = Actor.State(metaInfo, localHave)
        val remoteHave = HashSet[PieceId](0)
        val peerId     = Chunk.fill[Byte](20)(0)

        Actor.registerPeer(state, peerId)
        for {
          p      <- Promise.make[Throwable, AcquireJobResult]
          _      <- Actor.acquireJob(state, peerId, remoteHave, p)
          actual <- p.await.map(_.asInstanceOf[AcquireJobResult.DownloadCompleted.type])
        } yield assert(actual)(equalTo(AcquireJobResult.DownloadCompleted))
      },
      //
      testM("releases inactive job") {
        val state = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))

        val peerId = Chunk.fill[Byte](20)(0)
        val job    = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0))

        Actor.registerPeer(state, peerId)
        for {
          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job))
                      .fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(equalTo("DownloadJob(0,262144,0) is not in state.activeJobs"))))
      },
      //
      testM("releases completed job") {
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

        val job = DownloadJob(0, mi.pieceSize, mi.pieceHashes(0), mi.pieceSize)
        state.activeJobs.put(job, peerId)

        Actor.registerPeer(state, peerId)
        for {
          _ <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Downloading(job))
        } yield assert(state.activeJobs.size)(equalTo(0)) &&
          assert(state.localHave(0))(isTrue)
      },
      //
      testM("releases suspended job") {
        val state = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))

        val peerId = Chunk.fill[Byte](20)(0)

        val job = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize / 2)
        state.activeJobs.put(job, peerId)

        Actor.registerPeer(state, peerId)
        for {
          _ <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job))
        } yield //
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
