package torr.peerproc.test

import torr.actorsystem.{ActorSystem, ActorSystemLive}
import torr.channels.test.TestSocketChannel
import torr.directbuffers.DirectBufferPool
import torr.directbuffers.test.DirectBufferPoolMock
import torr.dispatcher.DownloadJob
import torr.fileio.test.FileIOMock
import zio._
import zio.test._
import zio.test.Assertion._
import torr.fileio.test.FileIOMock._
import torr.peerproc.{DefaultPeerRoutine, SequentialDownloadRoutine}
import torr.peerwire.{Message, ReceiveActor, PeerHandle}
import zio.clock.Clock
import zio.duration.durationInt
import zio.magic.ZioProvideMagicOps
import zio.nio.core._
import zio.test.environment.TestClock
import zio.test.mock.Expectation._
import zio.test.mock.MockSystem

object SequentialDownloadRoutineSpec extends DefaultRunnableSpec {
  def spec =
    suite("SequentialDownloadRoutineSpec")(
      testM("downloadUntilChokedOrCompleted - being choked before first request") {
        val effect = makePeerHandleHandle("Test").use {
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
          ActorSystemLive.make("Test"),
          FileIOMock.empty,
          DirectBufferPoolMock.empty
        )
      },
      //
      testM("downloadUntilChokedOrCompleted - stores first response") {
        val effect = makePeerHandleHandle("Test").use {
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
              assert(outgoing)(equalTo(0)) &&
              assert(job.getOffset)(equalTo(16))
        }

        val fileIOMock = FileIOMock.Store(anything, unit)

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          fileIOMock,
          DirectBufferPoolMock.empty
        )
      },
      //
      testM("downloadUntilChokedOrCompleted - downloads whole piece") {
        val effect = makePeerHandleHandle("Test").use {
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
          FileIOMock.Store(anything, unit) ++
            FileIOMock.Store(anything, unit)

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          fileIOMock,
          DirectBufferPoolMock.empty
        )
      },
      //
      testM("downloadUntilChokedOrCompleted - downloads whole piece (2 concurrent requests)") {
        val effect = makePeerHandleHandle("Test").use {
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
          FileIOMock.Store(anything, unit) ++
            FileIOMock.Store(anything, unit)

        effect.injectCustom(
          ActorSystemLive.make("Test"),
          fileIOMock,
          DirectBufferPoolMock.empty
        )
      },
      //
      testM("downloadUntilChokedOrCompleted - fails if pieces are received out of order") {
        val effect = makePeerHandleHandle("Test").use {
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
          ActorSystemLive.make("Test"),
          FileIOMock.empty,
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

  def makePeerHandleHandle(channelName: String): ZManaged[
    ActorSystem with DirectBufferPool with Clock,
    Throwable,
    (TestSocketChannel, PeerHandle)
  ] = {
    for {
      channel <- TestSocketChannel.make.toManaged_
      msgBuf  <- Buffer.byte(128).toManaged_
      handle  <- PeerHandle.fromChannel(channel, msgBuf, channelName, remotePeerId = Chunk.fill(20)(0.toByte))
    } yield (channel, handle)
  }

}
