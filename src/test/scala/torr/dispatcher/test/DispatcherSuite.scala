package torr.dispatcher.test

import torr.dispatcher.Actor.RegisteredPeer
import torr.dispatcher.{AcquireJobResult, Actor, DownloadCompletion, DownloadJob, PeerId, PieceId, ReleaseJobStatus}
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSuite.toBytes
import zio._
import zio.logging.Logging
import zio.magic.ZioProvideMagicOps
import zio.nio.core.{InetAddress, InetSocketAddress}
import zio.nio.core.file.Path
import zio.test._
import zio.test.Assertion._
import zio.test.DefaultRunnableSpec

import scala.collection.immutable.HashSet
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

object DispatcherSuite extends DefaultRunnableSpec {

  private def makeDefaultAddress =
    InetAddress.localHost.flatMap(InetSocketAddress.inetAddress(_, 12345))

  def spec =
    suite("DispatcherSuite")(
      //
      testM("registers peer") {
        val state  = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val peerId = Chunk.fill[Byte](20)(1)

        for {
          address <- makeDefaultAddress
          _        = Actor.registerPeer(state, peerId, address)
          expected = mutable.HashSet[PeerId](peerId)
        } yield assert(state.registeredPeers.keys)(equalTo(expected))
      },
      //
      testM("registers peer - fails if peer is already registered") {
        val state  = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val peerId = Chunk.fill[Byte](20)(100)

        for {
          address <- makeDefaultAddress
          _        = Actor.registerPeer(state, peerId, address)
          actual   = Try { Actor.registerPeer(state, peerId, address) }
        } yield assert(actual)(isFailure(hasMessage(equalTo("PeerId 00D620F6 is already registered"))))
      },
      //
      testM("unregisters peer - removes allocated job") {
        val peerId = Chunk.fill[Byte](20)(100)
        val job    = DownloadJob(0, metaInfo.pieceSize, Chunk.fill(20)(123.toByte))

        val effect =
          for {
            address <- makeDefaultAddress
            state    =
              Actor.State(
                metaInfo,
                new Array[Boolean](metaInfo.numPieces),
                registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address)),
                activeJobs = mutable.HashMap(job -> peerId),
                activePeers = mutable.HashMap(peerId -> ArrayBuffer(job))
              )

            _       <- Actor.unregisterPeer(state, peerId)
          } yield assert(state.registeredPeers.keys)(equalTo(new mutable.HashSet[PeerId])) &&
            assert(state.activeJobs)(equalTo(new mutable.HashMap[DownloadJob, PeerId]))

        effect.inject(Logging.ignore)
      },
      //
      testM("unregisters peer - frees upload slot") {
        val peerId = Chunk.fill[Byte](20)(100)

        val effect =
          for {
            address <- makeDefaultAddress
            state    =
              Actor.State(
                metaInfo,
                new Array[Boolean](metaInfo.numPieces),
                registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address)),
                uploadSlots = mutable.HashMap(peerId -> 1)
              )
            _       <- Actor.unregisterPeer(state, peerId)
          } yield assert(state.uploadSlots)(isEmpty)

        effect.inject(Logging.ignore)
      },
      //
      testM("unregisters peer - fails if peer is not registered") {
        val peerId = Chunk.fill[Byte](20)(100)

        val effect = for {
          address <- makeDefaultAddress
          state    = Actor.State(
                       metaInfo,
                       new Array[Boolean](metaInfo.numPieces),
                       registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address))
                     )

          _       <- Actor.unregisterPeer(state, peerId)
          actual  <- Actor.unregisterPeer(state, peerId).fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(equalTo("PeerId 00D620F6 is not registered"))))

        effect.inject(Logging.ignore)
      },
      //
      testM("acquire - first job") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](0, 1)

        val peerId   = Chunk.fill[Byte](20)(0)
        val expected = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0))

        for {
          address <- makeDefaultAddress
          _        = Actor.registerPeer(state, peerId, address)
          _        = Actor.reportHaveMany(state, peerId, remoteHave)
          actual  <- Actor.acquireJob(state, peerId)
                       .map(_.asInstanceOf[AcquireJobResult.Success].job)
        } yield //
        assert(actual)(equalTo(expected)) &&
          assert(state.activeJobs.size)(equalTo(1)) &&
          assert(state.activeJobs.head)(equalTo(expected, peerId))
      },
      //
      testM("acquire - second job") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](0, 1)

        val peerId         = Chunk.fill[Byte](20)(0)
        val expectedFirst  = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0))
        val expectedSecond = DownloadJob(1, metaInfo.pieceSize, metaInfo.pieceHashes(1))

        for {
          address <- makeDefaultAddress
          _        = Actor.registerPeer(state, peerId, address)
          _        = Actor.reportHaveMany(state, peerId, remoteHave)
          actual1 <- Actor.acquireJob(state, peerId)
                       .map(_.asInstanceOf[AcquireJobResult.Success].job)
          actual2 <- Actor.acquireJob(state, peerId)
                       .map(_.asInstanceOf[AcquireJobResult.Success].job)
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

        val remoteHave = HashSet[PieceId](0, 1)

        for {
          address <- makeDefaultAddress
          _        = Actor.registerPeer(state, peerId, address)
          _        = Actor.reportHaveMany(state, peerId, remoteHave)
          actual  <- Actor.acquireJob(state, peerId)
                       .map(_.asInstanceOf[AcquireJobResult.Success].job)
        } yield assert(actual)(equalTo(expected))
      },
      //
      testM("acquire - using remote have") {
        val state      = Actor.State(metaInfo, new Array[Boolean](metaInfo.numPieces))
        val remoteHave = HashSet[PieceId](1)

        val peerId   = Chunk.fill[Byte](20)(0)
        val expected = DownloadJob(1, metaInfo.pieceSize, metaInfo.pieceHashes(1))

        for {
          address <- makeDefaultAddress
          _        = Actor.registerPeer(state, peerId, address)
          _        = Actor.reportHaveMany(state, peerId, remoteHave)
          actual  <- Actor.acquireJob(state, peerId)
                       .map(_.asInstanceOf[AcquireJobResult.Success].job)
        } yield assert(actual)(equalTo(expected))

      },
      //
      testM("acquire - not interested") {
        val localHave  = new Array[Boolean](metaInfo.numPieces)
        localHave(0) = true
        val state      = Actor.State(metaInfo, localHave)
        val remoteHave = HashSet[PieceId](0)
        val peerId     = Chunk.fill[Byte](20)(0)

        for {
          address <- makeDefaultAddress
          _        = Actor.registerPeer(state, peerId, address)
          actual  <- Actor.acquireJob(state, peerId)
                       .map(_.asInstanceOf[AcquireJobResult.NoInterestingPieces.type])
        } yield assert(actual)(equalTo(AcquireJobResult.NoInterestingPieces))
      },
      //
      testM("acquire - not interested when download completed") {
        val localHave  = new Array[Boolean](metaInfo.numPieces)
        localHave(0) = true
        localHave(1) = true
        val state      = Actor.State(metaInfo, localHave)
        val remoteHave = HashSet[PieceId](0)
        val peerId     = Chunk.fill[Byte](20)(0)

        for {
          address <- makeDefaultAddress
          _        = Actor.registerPeer(state, peerId, address)
          actual  <- Actor.acquireJob(state, peerId)
                       .map(_.asInstanceOf[AcquireJobResult.NoInterestingPieces.type])
        } yield assert(actual)(equalTo(AcquireJobResult.NoInterestingPieces))
      },
      //
      testM("acquire - immediate success for active peer (on maxActivePeers)") {
        val peerId = Chunk.fill(3)('z'.toByte)

        val remoteHave = mutable.HashSet[PieceId](0)

        for {
          address <- makeDefaultAddress
          state    = Actor.State(
                       metaInfo,
                       localHave = new Array[Boolean](metaInfo.numPieces),
                       registeredPeers =
                         mutable.HashMap(peerId -> RegisteredPeer(address, have = remoteHave, interesting = remoteHave)),
                       maxSimultaneousDownloads = 1,
                       activePeers = mutable.HashMap(peerId -> ArrayBuffer[DownloadJob]())
                     )
          _        = state.localHave(0) = false
          _        = state.localHave(1) = true

          actual <- Actor.acquireJob(state, peerId)

        } yield assert(actual)(equalTo(
          AcquireJobResult.Success(
            DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0))
          )
        ))
      },
      //
      testM("acquire - returns OnQueue (on maxActivePeers)") {
        val peerId1 = Chunk.fill(3)('x'.toByte)
        val peerId2 = Chunk.fill(3)('y'.toByte)

        for {
          address <- makeDefaultAddress
          state    = Actor.State(
                       metaInfo,
                       localHave = new Array[Boolean](metaInfo.numPieces),
                       registeredPeers = mutable.HashMap(
                         peerId1 -> RegisteredPeer(address, mutable.HashSet[PieceId](0), mutable.HashSet[PieceId](0)),
                         peerId2 -> RegisteredPeer(address, mutable.HashSet[PieceId](0), mutable.HashSet[PieceId](0))
                       ),
                       maxSimultaneousDownloads = 1,
                       activePeers = mutable.HashMap(peerId1 -> ArrayBuffer[DownloadJob]())
                     )

          _        = state.localHave(0) = false
          _        = state.localHave(1) = true

          actual <- Actor.acquireJob(state, peerId2)
        } yield assert(actual)(equalTo(AcquireJobResult.OnQueue))
      },
      //
      testM("acquire - immediate success when active even if other peer is waiting") {
        val peerId1 = Chunk.fill(3)('x'.toByte)
        val peerId2 = Chunk.fill(3)('y'.toByte)

        val completedJob = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize)

        for {
          address <- makeDefaultAddress
          state    = Actor.State(
                       metaInfo,
                       localHave = new Array[Boolean](metaInfo.numPieces),
                       registeredPeers = mutable.HashMap(
                         peerId1 -> RegisteredPeer(address, mutable.HashSet(0, 1), mutable.HashSet(0, 1)),
                         peerId2 -> RegisteredPeer(address, mutable.HashSet(0, 1), mutable.HashSet(0, 1))
                       ),
                       maxSimultaneousDownloads = 1,
                       activePeers = mutable.HashMap(peerId1 -> ArrayBuffer(completedJob)),
                       activeJobs = mutable.HashMap[DownloadJob, PeerId](completedJob -> peerId1)
                     )

          _        = state.localHave(0) = false
          _        = state.localHave(1) = false

          _ <- Actor.acquireJob(state, peerId2) // other peer is waiting

          _       <- Actor.releaseJob( // releasing job with active status
                       state,
                       peerId1,
                       ReleaseJobStatus.Downloaded(completedJob)
                     )

          actual  <- Actor.acquireJob(state, peerId1) // active peer gets next job
          expected = AcquireJobResult.Success(DownloadJob(1, metaInfo.pieceSize, metaInfo.pieceHashes(1), 0))
        } yield assert(actual)(equalTo(expected)) &&
          assert(state.localHave(0))(isTrue) &&
          assert(state.localHave(1))(isFalse)
      },
      //
      testM("release - fails if peer is not active") {
        val peerId = Chunk.fill[Byte](3)('z'.toByte)
        val job    = DownloadJob(0, 32, Chunk[Byte](1, 2, 3))

        for {
          address <- makeDefaultAddress
          state    = Actor.State(
                       metaInfo,
                       localHave = new Array[Boolean](metaInfo.numPieces),
                       registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address))
                     )

          actual  <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job))
                       .fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(
          equalTo(
            "Peer F3ABB86B is releasing job Choked(DownloadJob(pieceId=0, length=32, offset=0)) while not being active"
          )
        )))
      },
      //
      testM("release - job has not been acquired") {
        val peerId = Chunk.fill[Byte](3)('z'.toByte)

        val job = DownloadJob(0, 32, Chunk[Byte](1, 2, 3))

        for {
          address <- makeDefaultAddress
          state    = Actor.State(
                       metaInfo,
                       localHave = new Array[Boolean](metaInfo.numPieces),
                       activePeers = mutable.HashMap(peerId -> ArrayBuffer[DownloadJob]()),
                       registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address))
                     )
          actual  <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job))
                       .fork.flatMap(_.await)
        } yield assert(actual)(fails(hasMessage(
          equalTo(
            "Peer F3ABB86B is releasing job Choked(DownloadJob(pieceId=0, length=32, offset=0)) that has not been acquired"
          )
        )))
      },
      //
      testM("release - active release, peer stays active") {
        val peerId = Chunk.fill[Byte](3)('z'.toByte)

        val currentJob = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize)

        for {
          address <- makeDefaultAddress
          state    = Actor.State(
                       metaInfo,
                       localHave = new Array[Boolean](metaInfo.numPieces),
                       registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address)),
                       activeJobs = mutable.HashMap(currentJob -> peerId),
                       activePeers = mutable.HashMap(peerId -> ArrayBuffer(currentJob))
                     )
          _        = state.localHave(0) = false
          _        = state.localHave(1) = false
          _       <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Downloaded(currentJob))
        } yield assert(())(anything) &&
          assert(state.activeJobs)(equalTo(mutable.HashMap[DownloadJob, PeerId]())) &&
          assert(state.activePeers)(equalTo(mutable.HashMap(peerId -> ArrayBuffer[DownloadJob]()))) && // stays active
          assert(state.localHave(0))(isTrue) &&
          assert(state.localHave(1))(isFalse)
      },
      //
      testM("release - choked release, peer becomes inactive") {
        val peerId     = Chunk.fill[Byte](3)('z'.toByte)
        val currentJob = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)

        for {
          address <- makeDefaultAddress

          state = Actor.State(
                    metaInfo,
                    localHave = new Array[Boolean](metaInfo.numPieces),
                    registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address)),
                    activeJobs = mutable.HashMap(currentJob -> peerId),
                    activePeers = mutable.HashMap(peerId -> ArrayBuffer(currentJob))
                  )

          _    <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(currentJob))
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

        for {
          address <- makeDefaultAddress

          state = Actor.State(
                    mi,
                    new Array[Boolean](metaInfo.numPieces),
                    registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address)),
                    activeJobs = mutable.Map(job -> peerId),
                    activePeers = mutable.HashMap(peerId -> ArrayBuffer(job))
                  )

          _    <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Downloaded(job))
        } yield assert(state.activeJobs.size)(equalTo(0)) &&
          assert(state.localHave(0))(isTrue)
      },
      //
      testM("release - active peer must not release incomplete jobs") {

        val peerId = Chunk.fill[Byte](20)(0)

        val incompleteJob = DownloadJob(0, metaInfo.pieceSize, Chunk[Byte](1, 2, 3), metaInfo.pieceSize - 1)

        for {
          address <- makeDefaultAddress

          state   = Actor.State(
                      metaInfo,
                      new Array[Boolean](metaInfo.numPieces),
                      registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address)),
                      activeJobs = mutable.Map(incompleteJob -> peerId),
                      activePeers = mutable.HashMap(peerId -> ArrayBuffer(incompleteJob))
                    )

          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Downloaded(incompleteJob))
                      .fork.flatMap(_.await)
        } yield assert(actual)(
          fails(hasMessage(equalTo(
            "ReleaseJobStatus status does not correspond to JobCompletionStatus: " +
              "(Downloaded(DownloadJob(pieceId=0, length=16, offset=15)), Incomplete)"
          )))
        )
      },
      //
      testM("release - choking peer must not release complete jobs") {

        val peerId      = Chunk.fill[Byte](20)(0)
        val completeJob = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize)

        for {
          address <- makeDefaultAddress

          state   = Actor.State(
                      metaInfo,
                      new Array[Boolean](metaInfo.numPieces),
                      registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address)),
                      activeJobs = mutable.Map(completeJob -> peerId),
                      activePeers = mutable.HashMap(peerId -> ArrayBuffer(completeJob))
                    )

          actual <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(completeJob))
                      .fork.flatMap(_.await)
        } yield assert(actual)(
          fails(hasMessage(equalTo(
            "ReleaseJobStatus status does not correspond to JobCompletionStatus: " +
              "(Choked(DownloadJob(pieceId=0, length=16, offset=16)), Verified)"
          )))
        )
      },
      //
      testM("release - incomplete job becomes suspended") {
        val peerId = Chunk.fill[Byte](20)(0)
        val job    = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), metaInfo.pieceSize / 2)

        for {
          address <- makeDefaultAddress

          state = Actor.State(
                    metaInfo,
                    localHave = new Array[Boolean](metaInfo.numPieces),
                    registeredPeers = mutable.HashMap(peerId -> RegisteredPeer(address)),
                    activeJobs = mutable.HashMap(job -> peerId),
                    activePeers = mutable.HashMap(peerId -> ArrayBuffer(job))
                  )

          _    <- Actor.releaseJob(state, peerId, ReleaseJobStatus.Choked(job))
        } yield //
        assert(state.activeJobs.size)(equalTo(0)) &&
          assert(state.localHave(0))(isFalse) &&
          assert(state.suspendedJobs.size)(equalTo(1)) &&
          assert(state.suspendedJobs.head)(equalTo((0, job)))
      },
      testM("unregisterPeer - jobs become available to other peers") {

        val peerId1 = Chunk.fill[Byte](20)(101)
        val peerId2 = Chunk.fill[Byte](20)(102)

        val job1 = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)
        val job2 = DownloadJob(1, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)

        val peerHave = mutable.HashSet(0, 1)

        val effect =
          for {

            address <- makeDefaultAddress

            state               = Actor.State(
                                    metaInfo,
                                    new Array[Boolean](metaInfo.numPieces),
                                    registeredPeers =
                                      mutable.HashMap(
                                        peerId1 -> RegisteredPeer(address, have = peerHave, interesting = peerHave),
                                        peerId2 -> RegisteredPeer(address, have = peerHave, interesting = peerHave)
                                      ),
                                    activeJobs = mutable.Map(job1 -> peerId1, job2 -> peerId1),
                                    activePeers = mutable.HashMap(peerId1 -> ArrayBuffer(job1, job2))
                                  )

            _                  <- Actor.unregisterPeer(state, peerId1)
            downloadCompletion  = Actor.isDownloadCompleted(state)
            isRemoteInteresting = Actor.isRemoteInteresting(state, peerId2)
            actualJob          <- Actor.acquireJob(state, peerId2)

          } yield assert(downloadCompletion)(equalTo(DownloadCompletion.InProgress)) &&
            assert(isRemoteInteresting)(isTrue) &&
            assert(state.suspendedJobs)(equalTo(mutable.HashMap(1 -> job2))) &&
            assert(actualJob)(equalTo(AcquireJobResult.Success(job1))) &&
            assert(state.localHave)(equalTo(Array(false, false)))

        effect.inject(Logging.ignore)
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
