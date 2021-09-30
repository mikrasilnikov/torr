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
      testM("acquire - first job") {
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
      testM("acquire - second job") {
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
      testM("acquire - allocates suspended job") {
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
      testM("acquire - using remote have") {
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
      testM("acquire - not interested") {
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
      testM("acquire - download completed") {
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
      testM("acquire - fails if a peer is already waiting or a job") {
        val peerId     = Chunk.fill(3)('z'.toByte)
        val localHave  = new Array[Boolean](metaInfo.numPieces)
        val state      = Actor.State(metaInfo, localHave, registeredPeers = mutable.Set(peerId))
        val remoteHave = HashSet[PieceId](0)

        for {
          p1 <- Promise.make[Throwable, AcquireJobResult]
          _   = state.pendingJobRequests.append(AcquireJobRequest(peerId, remoteHave, p1))

          p2     <- Promise.make[Throwable, AcquireJobResult]
          _      <- Actor.acquireJob(state, peerId, remoteHave, p2)
          actual <- p2.await.fork.flatMap(_.await)

        } yield assert(actual)(fails(hasMessage(equalTo("Peer zzz is already waiting for a job"))))
      },
      //
      testM("acquire - immediate success for active peer (on maxActivePeers)") {
        val peerId = Chunk.fill(3)('z'.toByte)

        val state = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.Set(peerId),
          maxActivePeers = 1,
          activePeers = mutable.HashMap(peerId -> ArrayBuffer[DownloadJob]())
        )

        state.localHave(0) = false
        state.localHave(1) = true

        val remoteHave = HashSet[PieceId](0)

        for {
          p      <- Promise.make[Throwable, AcquireJobResult]
          _      <- Actor.acquireJob(state, peerId, remoteHave, p)
          actual <- p.await

        } yield assert(actual)(equalTo(
          AcquireJobResult.AcquireSuccess(
            DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0))
          )
        ))
      },
      //
      testM("acquire - suspends request for inactive peer (on maxActivePeers)") {
        val peerId1 = Chunk.fill(3)('x'.toByte)
        val peerId2 = Chunk.fill(3)('y'.toByte)

        val state = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.Set(peerId1, peerId2),
          maxActivePeers = 1,
          activePeers = mutable.HashMap(peerId1 -> ArrayBuffer[DownloadJob]())
        )

        state.localHave(0) = false
        state.localHave(1) = true

        val remoteHave = HashSet[PieceId](0)

        for {
          p     <- Promise.make[Throwable, AcquireJobResult]
          _     <- Actor.acquireJob(state, peerId2, remoteHave, p)
          actual = state.pendingJobRequests
        } yield assert(actual)(equalTo(ArrayBuffer(AcquireJobRequest(peerId2, remoteHave, p))))
      },
      //
      testM("acquire - delayed success (when releaser is choked)") {
        val peerId1 = Chunk.fill(3)('x'.toByte)
        val peerId2 = Chunk.fill(3)('y'.toByte)

        val job = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)

        val state = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.Set(peerId1, peerId2),
          maxActivePeers = 1,
          activePeers = mutable.HashMap(peerId1 -> ArrayBuffer(job)),
          activeJobs = mutable.HashMap[DownloadJob, PeerId](job -> peerId1)
        )

        state.localHave(0) = false
        state.localHave(1) = false

        val remoteHave = HashSet[PieceId](0, 1)

        for {
          p       <- Promise.make[Throwable, AcquireJobResult]
          _       <- Actor.acquireJob(state, peerId2, remoteHave, p)
          enqueued = state.pendingJobRequests.exists(req => req.peerId == peerId2 && req.promise == p)
          _       <- Actor.releaseJob(state, peerId1, ReleaseJobStatus.Choked(job))
          actual  <- p.await
        } yield assert(enqueued)(isTrue) && assert(actual)(equalTo(AcquireJobResult.AcquireSuccess(job)))
      },
      //
      testM("acquire - immediate success when active even if other peer is waiting") {
        val peerId1 = Chunk.fill(3)('x'.toByte)
        val peerId2 = Chunk.fill(3)('y'.toByte)

        val completedJob = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize)

        val state = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.Set(peerId1, peerId2),
          maxActivePeers = 1,
          activePeers = mutable.HashMap(peerId1 -> ArrayBuffer(completedJob)),
          activeJobs = mutable.HashMap[DownloadJob, PeerId](completedJob -> peerId1)
        )

        state.localHave(0) = false
        state.localHave(1) = false

        val remoteHave = HashSet[PieceId](0, 1)

        for {
          p0 <- Promise.make[Throwable, AcquireJobResult]
          _  <- Actor.acquireJob(state, peerId2, remoteHave, p0) // other peer is waiting

          _       <- Actor.releaseJob( // releasing job with active status
                       state,
                       peerId1,
                       ReleaseJobStatus.Active(completedJob)
                     )

          p1      <- Promise.make[Throwable, AcquireJobResult]
          _       <- Actor.acquireJob(state, peerId1, remoteHave, p1) // active peer gets next job
          expected = AcquireJobResult.AcquireSuccess(DownloadJob(1, metaInfo.pieceSize, metaInfo.pieceHashes(1), 0))
          actual  <- p1.await
        } yield assert(actual)(equalTo(expected)) &&
          assert(state.localHave(0))(isTrue) &&
          assert(state.localHave(1))(isFalse)
      },
      //
      testM("release - fails if peer is not active") {
        val peerId = Chunk.fill[Byte](3)('z'.toByte)
        val state  = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet[PeerId](peerId)
        )

        val job = DownloadJob(0, 32, Chunk[Byte](1, 2, 3))

        for {
          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job))
                      .fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(
          equalTo("Peer zzz is releasing job Choked(DownloadJob(0,32,Chunk(1,2,3),0)) while not being active")
        )))
      },
      //
      testM("release - job has not been acquired") {
        val peerId = Chunk.fill[Byte](3)('z'.toByte)
        val state  = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer[DownloadJob]()),
          registeredPeers = mutable.HashSet[PeerId](peerId)
        )

        val job = DownloadJob(0, 32, Chunk[Byte](1, 2, 3))

        for {
          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job))
                      .fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(
          equalTo("Peer zzz is releasing job Choked(DownloadJob(0,32,Chunk(1,2,3),0)) that has not been acquired")
        )))
      },
      //
      testM("release - active release, peer stays active") {
        val peerId = Chunk.fill[Byte](3)('z'.toByte)

        val currentJob = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize)
        val state      = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet[PeerId](peerId),
          activeJobs = mutable.HashMap(currentJob -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(currentJob))
        )
        state.localHave(0) = false
        state.localHave(1) = false

        for {
          _ <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Active(currentJob))
        } yield assert(())(anything) &&
          assert(state.activeJobs)(equalTo(mutable.HashMap[DownloadJob, PeerId]())) &&
          assert(state.activePeers)(equalTo(mutable.HashMap(peerId -> ArrayBuffer[DownloadJob]()))) && // stays active
          assert(state.localHave(0))(isTrue) &&
          assert(state.localHave(1))(isFalse)
      },
      //
      testM("release - choked release, fails on > 1 active jobs for peer") {
        val peerId = Chunk.fill[Byte](3)('z'.toByte)

        val job1 = DownloadJob(0, metaInfo.pieceSize, Chunk[Byte](1, 2, 3), 0)
        val job2 = DownloadJob(1, metaInfo.pieceSize, Chunk[Byte](4, 5, 6), 0)

        val state = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet[PeerId](peerId),
          activeJobs = mutable.HashMap(job1 -> peerId, job2 -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(job1, job2))
        )

        for {
          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job1))
                      .fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(equalTo(
          "Peer zzz is releasing Choked(DownloadJob(0,16,Chunk(1,2,3),0)) while having multiple jobs allocated: " +
            "DownloadJob(0,16,Chunk(1,2,3),0),DownloadJob(1,16,Chunk(4,5,6),0)"
        ))))
      },
      //
      testM("release - choked release, peer becomes inactive") {
        val peerId = Chunk.fill[Byte](3)('z'.toByte)

        val currentJob = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)
        val state      = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet[PeerId](peerId),
          activeJobs = mutable.HashMap(currentJob -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(currentJob))
        )

        for {
          _ <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(currentJob))
        } yield assert(())(anything) &&
          assert(state.activeJobs)(equalTo(mutable.HashMap[DownloadJob, PeerId]())) &&
          assert(state.activePeers)(equalTo(mutable.HashMap[PeerId, ArrayBuffer[DownloadJob]]())) && // becomes inactive
          assert(state.localHave(0))(isFalse) &&
          assert(state.localHave(1))(isFalse) &&
          assert(state.suspendedJobs)(
            equalTo(mutable.HashMap(currentJob.pieceId -> currentJob)) // job becomes suspended
          )
      },
      //
      testM("release - (Downloading) fails if a peer has a pending job request") {
        val peerId     = Chunk.fill[Byte](3)('z'.toByte)
        val remoteHave = HashSet[PieceId](0, 1)
        val job        = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)

        val state = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet[PeerId](peerId),
          maxActivePeers = 1,
          activeJobs = mutable.HashMap(job -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(job))
        )

        for {
          requestPromise <- Promise.make[Throwable, AcquireJobResult]

          _ = state.pendingJobRequests.append(AcquireJobRequest(
                peerId,
                remoteHave,
                requestPromise
              )) // has pending request

          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Active(job))
                      .fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(
          equalTo("Active peer zzz can not have a pending job request")
        )))
      },
      //
      testM("release - (Choked) fails if a peer has a pending job request") {
        val peerId     = Chunk.fill[Byte](3)('z'.toByte)
        val remoteHave = HashSet[PieceId](0, 1)
        val job        = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)

        val state = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet[PeerId](peerId),
          maxActivePeers = 1,
          activeJobs = mutable.HashMap(job -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(job))
        )

        for {
          requestPromise <- Promise.make[Throwable, AcquireJobResult]

          _ = state.pendingJobRequests.append(AcquireJobRequest(
                peerId,
                remoteHave,
                requestPromise
              )) // has pending request

          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job))
                      .fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(
          equalTo("Active peer zzz can not have a pending job request")
        )))
      },
      //
      testM("release - completed job, updates localHave") {
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

        val job = DownloadJob(0, mi.pieceSize, mi.pieceHashes(0), mi.pieceSize)

        val state = Actor.State(
          mi,
          new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet(peerId),
          activeJobs = mutable.Map(job -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(job))
        )

        for {
          _ <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Active(job))
        } yield assert(state.activeJobs.size)(equalTo(0)) &&
          assert(state.localHave(0))(isTrue)
      },
      //
      testM("release - active peer must not release incomplete jobs") {

        val peerId = Chunk.fill[Byte](20)(0)

        val incompleteJob = DownloadJob(0, metaInfo.pieceSize, Chunk[Byte](1, 2, 3), metaInfo.pieceSize - 1)

        val state = Actor.State(
          metaInfo,
          new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet(peerId),
          activeJobs = mutable.Map(incompleteJob -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(incompleteJob))
        )

        for {
          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Active(incompleteJob))
                      .fork.flatMap(_.await)
        } yield assert(actual)(
          fails(hasMessage(equalTo(
            "ReleaseJobStatus status does not correspond to JobCompletionStatus: " +
              "(Active(DownloadJob(0,16,Chunk(1,2,3),15)), Incomplete)"
          )))
        )
      },
      //
      testM("release - choking peer must not release complete jobs") {

        val peerId = Chunk.fill[Byte](20)(0)

        val completeJob = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize)

        val state = Actor.State(
          metaInfo,
          new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet(peerId),
          activeJobs = mutable.Map(completeJob -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(completeJob))
        )

        for {
          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(completeJob))
                      .fork.flatMap(_.await)
        } yield assert(actual)(
          fails(hasMessage(equalTo(
            "ReleaseJobStatus status does not correspond to JobCompletionStatus: " +
              "(Choked(DownloadJob(0,16,Chunk(-38,57,-93,-18,94,107,75,13,50,85,-65,-17,-107,96,24,-112,-81,-40,7,9),16)), Complete)"
          )))
        )
      },
      //
      testM("release - incomplete job becomes suspended") {
        val peerId = Chunk.fill[Byte](20)(0)
        val job    = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize / 2)

        val state = Actor.State(
          metaInfo,
          localHave = new Array[Boolean](metaInfo.numPieces),
          registeredPeers = mutable.HashSet(peerId),
          activeJobs = mutable.HashMap(job -> peerId),
          activePeers = mutable.HashMap(peerId -> ArrayBuffer(job))
        )

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
    pieceSize = 16,
    entries = FileEntry(Path("file1.dat"), 32) :: Nil,
    pieceHashes = Vector(
      // hashes of empty array
      toBytes("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
      toBytes("da39a3ee5e6b4b0d3255bfef95601890afd80709")
    ),
    infoHash = Chunk[Byte](1, 2, 3)
  )
}
