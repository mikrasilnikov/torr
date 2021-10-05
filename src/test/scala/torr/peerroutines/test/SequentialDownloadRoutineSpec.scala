package torr.peerroutines.test

import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.channels.test.TestSocketChannel
import torr.consoleui.NullConsoleUI
import torr.directbuffers.DirectBufferPool
import torr.directbuffers.test.DirectBufferPoolMock
import torr.dispatcher.{Dispatcher, DispatcherLive, DownloadJob}
import torr.fileio.test.FileIOMock
import torr.metainfo.{FileEntry, MetaInfo}
import torr.metainfo.test.MetaInfoSpec.toBytes
import torr.peerroutines.SequentialDownloadRoutine
import torr.peerwire.test.PeerHandleLiveSpec.metaInfo
import torr.peerwire.{Message, PeerHandle, PeerHandleLive}
import zio._
import zio.clock.Clock
import zio.magic.ZioProvideMagicOps
import zio.nio.core._
import zio.nio.core.file.Path
import zio.test.Assertion._
import zio.test._
import zio.test.mock.Expectation._

object SequentialDownloadRoutineSpec extends DefaultRunnableSpec {
  def spec =
    suite("SequentialDownloadRoutineSpec")(
      testM("downloadUntilChokedOrCompleted - being choked before first request") {
        val effect = makePeerHandle("Test").use {
          case (channel, handle) =>
            for {
              _        <- handle.onMessage(Message.Choke)
              actual   <- SequentialDownloadRoutine.downloadUntilChokedOrCompleted(
                            handle,
                            DownloadJob(0, 128, Chunk.fill(20)(123.toByte)),
                            maxConcurrentRequests = 1
                          )
              outgoing <- channel.outgoingSize
            } yield assert(actual)(isTrue) && assert(outgoing)(equalTo(0))
        }

        effect.injectCustom(
          DispatcherLive.make,
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DirectBufferPoolMock.empty
        )
      },
      //
      testM("downloadUntilChokedOrCompleted - stores first response") {
        val effect = makePeerHandle("Test").use {
          case (channel, handle) =>
            for {
              buf       <- Buffer.byte(1024)
              job        = DownloadJob(0, 128, Chunk.fill(20)(123.toByte))
              handleFib <- SequentialDownloadRoutine.downloadUntilChokedOrCompleted(
                             handle,
                             job,
                             maxConcurrentRequests = 1,
                             requestSize = 16
                           ).fork

              msg       <- Message.receive(channel.remote, buf)
              req        = msg.asInstanceOf[Message.Request]
              payload   <- Buffer.byte(req.length)
              _         <- handle.onMessage(Message.Piece(req.index, 0, payload))
              _         <- handle.onMessage(Message.Choke)
              choked    <- handleFib.join

              outgoing <- channel.outgoingSize
            } yield assert(choked)(isTrue) &&
              //assert(outgoing)(equalTo(0)) &&
              assert(job.getOffset)(equalTo(16))
        }

        val fileIOMock =
          FileIOMock.MetaInfo(value(metaInfo)) ++
            FileIOMock.FreshFilesWereAllocated(value(true)) ++
            FileIOMock.Store(anything, unit)

        effect.injectCustom(
          DispatcherLive.make,
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          fileIOMock,
          DirectBufferPoolMock.empty
        )
      },
      //
      testM("downloadUntilChokedOrCompleted - downloads whole piece") {
        val effect = makePeerHandle("Test").use {
          case (channel, handle) =>
            for {
              buf       <- Buffer.byte(1024)
              job        = DownloadJob(0, 31, Chunk.fill(20)(123.toByte))
              handleFib <- SequentialDownloadRoutine.downloadUntilChokedOrCompleted(
                             handle,
                             job,
                             maxConcurrentRequests = 1,
                             requestSize = 16
                           ).fork

              req1      <- Message.receive(channel.remote, buf).map(_.asInstanceOf[Message.Request])
              rBuf1     <- Buffer.byte(req1.length)
              _         <- handle.onMessage(Message.Piece(req1.index, req1.offset, rBuf1))
              req2      <- Message.receive(channel.remote, buf).map(_.asInstanceOf[Message.Request])
              rBuf2     <- Buffer.byte(req2.length)
              _         <- handle.onMessage(Message.Piece(req2.index, req2.offset, rBuf2))

              choked <- handleFib.join

              outgoing <- channel.outgoingSize
            } yield assert(choked)(isFalse) &&
              assert(req1)(equalTo(Message.Request(0, 0, 16))) &&
              assert(req2)(equalTo(Message.Request(0, 16, 15))) &&
              assert(outgoing)(equalTo(0)) &&
              assert(job.getOffset)(equalTo(job.length))
        }

        val fileIOMock =
          FileIOMock.MetaInfo(value(metaInfo)) ++
            FileIOMock.FreshFilesWereAllocated(value(true)) ++
            FileIOMock.Store(anything, unit) ++
            FileIOMock.Store(anything, unit)

        effect.injectCustom(
          DispatcherLive.make,
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          fileIOMock,
          DirectBufferPoolMock.empty
        )
      },
      //
      testM("downloadUntilChokedOrCompleted - downloads whole piece (2 concurrent requests)") {
        val effect = makePeerHandle("Test").use {
          case (channel, handle) =>
            for {
              buf       <- Buffer.byte(1024)
              job        = DownloadJob(0, 31, Chunk.fill(20)(123.toByte))
              handleFib <- SequentialDownloadRoutine.downloadUntilChokedOrCompleted(
                             handle,
                             job,
                             maxConcurrentRequests = 2,
                             requestSize = 16
                           ).fork

              req1      <- Message.receive(channel.remote, buf).map(_.asInstanceOf[Message.Request])
              req2      <- Message.receive(channel.remote, buf).map(_.asInstanceOf[Message.Request])
              rBuf1     <- Buffer.byte(req1.length)
              _         <- handle.onMessage(Message.Piece(req1.index, req1.offset, rBuf1))
              rBuf2     <- Buffer.byte(req2.length)
              _         <- handle.onMessage(Message.Piece(req2.index, req2.offset, rBuf2))

              choked <- handleFib.join

              outgoing <- channel.outgoingSize
            } yield assert(choked)(isFalse) &&
              assert(req1)(equalTo(Message.Request(0, 0, 16))) &&
              assert(req2)(equalTo(Message.Request(0, 16, 15))) &&
              assert(outgoing)(equalTo(0)) &&
              assert(job.getOffset)(equalTo(job.length))
        }

        val fileIOMock =
          FileIOMock.MetaInfo(value(metaInfo)) ++
            FileIOMock.FreshFilesWereAllocated(value(true)) ++
            FileIOMock.Store(anything, unit) ++
            FileIOMock.Store(anything, unit)

        effect.injectCustom(
          DispatcherLive.make,
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          fileIOMock,
          DirectBufferPoolMock.empty
        )
      },
      //
      testM("downloadUntilChokedOrCompleted - fails if pieces are received out of order") {
        val effect = makePeerHandle("Test").use {
          case (channel, handle) =>
            for {
              buf       <- Buffer.byte(1024)
              job        = DownloadJob(0, 31, Chunk.fill(20)(123.toByte))
              handleFib <- SequentialDownloadRoutine.downloadUntilChokedOrCompleted(
                             handle,
                             job,
                             maxConcurrentRequests = 2,
                             requestSize = 16
                           ).fork

              req1      <- Message.receive(channel.remote, buf).map(_.asInstanceOf[Message.Request])
              req2      <- Message.receive(channel.remote, buf).map(_.asInstanceOf[Message.Request])
              rBuf1     <- Buffer.byte(req1.length)
              rBuf2     <- Buffer.byte(req2.length)
              _         <- handle.onMessage(Message.Piece(req2.index, req2.offset, rBuf2))
              _         <- handle.onMessage(Message.Piece(req1.index, req1.offset, rBuf1))

              actual <- handleFib.await

              outgoing <- channel.outgoingSize
            } yield assert(actual)(
              fails(hasMessage(containsString(
                "Received piece does not correspond to sent request."
              )))
            )
        }

        effect.injectCustom(
          DispatcherLive.make,
          NullConsoleUI.make,
          ActorSystemLive.make("Test"),
          FileIOMock.MetaInfo(value(metaInfo)) ++ FileIOMock.FreshFilesWereAllocated(value(true)),
          DirectBufferPoolMock.empty
        )
      },
      //
      test("123") {
        val l0 = FileIOMock.Fetch(
          equalTo(0, 0, 128),
          valueM { case (_, _, amount) => Buffer.byte(amount).map(Chunk(_)) }
        )
        val l1 = FileIOMock.Store(anything, unit)

        assert(())(anything)
      }
    )

  def makePeerHandle(channelName: String): ZManaged[
    Dispatcher with ActorSystem with DirectBufferPool with Clock,
    Throwable,
    (TestSocketChannel, PeerHandle)
  ] = {
    for {
      channel <- TestSocketChannel.make.toManaged_
      msgBuf  <- Buffer.byte(128).toManaged_
      handle  <- PeerHandleLive.fromChannel(channel, msgBuf, channelName, remotePeerId = Chunk.fill(20)(0.toByte))
    } yield (channel, handle)
  }

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
