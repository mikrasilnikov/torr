package torr.peerroutines.test

import torr.directbuffers.DirectBufferPool
import torr.directbuffers.test.DirectBufferPoolMock
import torr.dispatcher.{AcquireJobResult, Dispatcher, DownloadJob, PieceId, ReleaseJobStatus}
import torr.dispatcher.Actor.AcquireJobRequest
import torr.dispatcher.test.DispatcherSpec.metaInfo
import torr.fileio.FileIO
import torr.fileio.test.FileIOMock
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSpec.toBytes
import torr.peerroutines.DefaultPeerRoutine.DownloadState
import torr.peerroutines.PipelineDownloadRoutine
import torr.peerwire.Message
import torr.peerwire.test.PeerHandleAndDispatcherMock
import zio._
import zio.test._
import zio.test.Assertion._
import torr.peerwire.test.PeerHandleAndDispatcherMock._
import zio.clock.Clock
import zio.magic.ZioProvideMagicOps
import zio.nio.core.file.Path

import scala.collection.immutable
import scala.collection.immutable.HashSet

object PipelineDownloadRoutineSpec extends DefaultRunnableSpec {
  def spec =
    suite("PipelineDownloadRoutineSpec")(
      testM("Successful download") {

        val peerId     = Chunk.fill(3)('z'.toByte)
        val remoteHave = immutable.HashSet[PieceId](0, 1, 2)
        val job1       = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)
        val job2       = DownloadJob(1, metaInfo.pieceSize, metaInfo.pieceHashes(1), 0)

        val expectations =
          IsDownloadCompleted(false) ::
            IsRemoteInteresting(remoteHave, true) ::
            Send(Message.Interested) ::
            Receive(Message.Unchoke) ::
            Fork(
              left =
                AcquireJob(peerId, remoteHave, AcquireJobResult.AcquireSuccess(job1)) ::
                  Poll(None) ::
                  Send(Message.Request(0, 0, 16 * 1024)) ::
                  Send(Message.Request(0, 16 * 1024, 16 * 1024)) ::
                  AcquireJob(peerId, remoteHave, AcquireJobResult.AcquireSuccess(job2)) ::
                  Send(Message.Request(1, 0, 16 * 1024)) ::
                  Send(Message.Request(1, 16 * 1024, 16 * 1024)) ::
                  AcquireJob(peerId, remoteHave, AcquireJobResult.NotInterested) ::
                  Nil,
              right =
                ReceiveBlock(0, 0, 16 * 1024) ::
                  ReceiveBlock(0, 16 * 1024, 16 * 1024) ::
                  ReceiveBlock(1, 0, 16 * 1024) ::
                  ReleaseJob(peerId, ReleaseJobStatus.Active(job1)) ::
                  ReceiveBlock(1, 16 * 1024, 16 * 1024) ::
                  ReleaseJob(peerId, ReleaseJobStatus.Active(job2)) ::
                  Nil
            ) ::
            Send(Message.NotInterested) ::
            IsDownloadCompleted(true) ::
            Nil

        val effect = PeerHandleAndDispatcherMock.makeMocks(peerId, expectations).flatMap {
          case (dispatcher, peerHandle) =>
            val initialState = DownloadState(peerChoking = true, amInterested = false)
            for {
              remoteHaveRef <- Ref.make[Set[PieceId]](remoteHave)

              _ <- PipelineDownloadRoutine
                     .download(peerHandle, remoteHaveRef, initialState, maxConcurrentRequests = 1)
                     .provideSomeLayer[Clock with FileIO with DirectBufferPool](ZLayer.succeed(dispatcher))

            } yield assert(())(anything)
        }

        val fileIOMock =
          FileIOMock.Store(anything) ++
            FileIOMock.Store(anything) ++
            FileIOMock.Store(anything) ++
            FileIOMock.Store(anything)

        effect.injectCustom(fileIOMock, DirectBufferPoolMock.empty)

      },
      testM("Being choked before sending first request") {

        val peerId     = Chunk.fill(3)('z'.toByte)
        val remoteHave = immutable.HashSet[PieceId](0, 1, 2)
        val job        = DownloadJob(0, metaInfo.pieceSize, metaInfo.pieceHashes(0), 0)

        val expectations =
          IsDownloadCompleted(false) ::
            IsRemoteInteresting(remoteHave, true) ::
            Send(Message.Interested) ::
            Receive(Message.Unchoke) ::
            AcquireJob(peerId, remoteHave, AcquireJobResult.AcquireSuccess(job)) ::
            Poll(Some(Message.Choke)) ::
            ReleaseJob(peerId, ReleaseJobStatus.Choked(job)) ::
            IsDownloadCompleted(true) ::
            Nil

        val effect = PeerHandleAndDispatcherMock.makeMocks(peerId, expectations).flatMap {
          case (dispatcher, peerHandle) =>
            val initialState = DownloadState(peerChoking = true, amInterested = false)
            for {
              remoteHaveRef <- Ref.make[Set[PieceId]](remoteHave)

              _ <- PipelineDownloadRoutine
                     .download(peerHandle, remoteHaveRef, initialState, maxConcurrentRequests = 1)
                     .provideSomeLayer[Clock with FileIO with DirectBufferPool](ZLayer.succeed(dispatcher))

            } yield assert(())(anything)
        }

        effect.injectCustom(FileIOMock.empty, DirectBufferPoolMock.empty)

      }
    )

  private val metaInfo = MetaInfo(
    announce = "udp://tracker.openbittorrent.com:80/announce",
    pieceSize = 2 * 16 * 1024,
    entries = FileEntry(Path("file1.dat"), 4 * 16 * 1024) :: Nil,
    pieceHashes = Vector(
      // hashes of empty array
      toBytes("da39a3ee5e6b4b0d3255bfef95601890afd80709"),
      toBytes("da39a3ee5e6b4b0d3255bfef95601890afd80709")
    ),
    infoHash = Chunk[Byte](1, 2, 3)
  )
}
