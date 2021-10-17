package torr.peerwire.test

import torr.dispatcher
import torr.dispatcher.{
  AcquireJobResult,
  AcquireUploadSlotResult,
  Dispatcher,
  DownloadCompletion,
  PeerId,
  PieceId,
  ReleaseJobStatus
}
import torr.peerwire.{Message, PeerHandle, TorrBitSet}
import zio._
import zio.nio.core.Buffer

import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag

object PeerHandleAndDispatcherMock {

  sealed trait Expectation[+_]
  case class Send(msg: Message)                                          extends Expectation[Unit]
  case class Receive(msg: Message)                                       extends Expectation[Message]
  case class ReceiveBlock(pieceId: Int, offset: Int, size: Int)          extends Expectation[Message]
  case class Poll(result: Option[Message])                               extends Expectation[Option[Message]]
  case class IsDownloadCompleted(result: DownloadCompletion)             extends Expectation[DownloadCompletion]
  case class IsRemoteInteresting(peerId: PeerId, result: Boolean)        extends Expectation[Boolean]
  case class AcquireJob(peerId: PeerId, result: AcquireJobResult)        extends Expectation[AcquireJobResult]
  case class ReleaseJob(peerId: PeerId, releaseStatus: ReleaseJobStatus) extends Expectation[Unit]

  def makeMocks(mockPeerId: PeerId, expectations: List[Expectation[_]]): UIO[(Dispatcher.Service, PeerHandle)] = {

    RefM.make(expectations).map { expectationsRef =>
      val dispatcher = new Dispatcher.Service {

        def isDownloadCompleted: Task[DownloadCompletion] = {
          expectationsRef.modify {
            case IsDownloadCompleted(res) :: tail => ZIO.succeed(res, tail)
            case exp                              =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got isDownloadCompleted()"))
          }
        }

        def isRemoteInteresting(peerId: PeerId): Task[Boolean] = {
          expectationsRef.modify {
            case IsRemoteInteresting(pid, res) :: tail if (peerId == pid) =>
              ZIO.succeed(res, tail)
            case exp                                                      =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got isRemoteInteresting($peerId)"))
          }
        }

        def acquireJob(peerId: PeerId): Task[AcquireJobResult] = {
          expectationsRef.modify {
            case AcquireJob(pid, res) :: tail if peerId == pid =>
              ZIO.succeed(res, tail)
            case exp                                           =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got acquireJob($peerId)"))
          }
        }

        def releaseJob(peerId: PeerId, releaseStatus: => ReleaseJobStatus): Task[Unit] =
          expectationsRef.modify {
            case ReleaseJob(pid, rs) :: tail if peerId == pid && releaseStatus == rs => ZIO.succeed((), tail)
            case exp                                                                 =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got releaseJob($peerId, $releaseStatus)"))
          }

        override def acquireUploadSlot(peerId: PeerId): Task[AcquireUploadSlotResult] = ???
        override def releaseUploadSlot(peerId: PeerId): Task[Unit]                    = ???

        def registerPeer(peerId: PeerId): Task[Unit]   = ???
        def unregisterPeer(peerId: PeerId): Task[Unit] = ???

        def reportHave(peerId: PeerId, piece: PieceId): Task[Unit]           = ???
        def reportHaveMany(peerId: PeerId, pieces: Set[PieceId]): Task[Unit] = ???

        def reportDownloadSpeed(peerId: PeerId, bytesPerSecond: PieceId): Task[Unit] = ???
        def reportUploadSpeed(peerId: PeerId, bytesPerSecond: PieceId): Task[Unit]   = ???

        def subscribeToHaveUpdates(peerId: PeerId): Task[(Set[PieceId], Dequeue[PieceId])] = ???

        def numActivePeers: Task[PieceId] = ???
      }

      val peerHandle = new PeerHandle {

        def send(msg: Message): Task[Unit] = {
          //ZIO(println(s"-> $msg")) *>
          expectationsRef.modify {
            case Send(m) :: tail if msg == m => ZIO.succeed((), tail)
            case exp                         =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got send($msg)"))
          }
        }

        def receive[M <: Message](implicit tag: ClassTag[M]): Task[M] = {
          expectationsRef.modify {
            case ReceiveBlock(p, o, s) :: tail                        =>
              Buffer.byte(s).map(b => (Message.Piece(p, o, b).asInstanceOf[M], tail))
            case Receive(m) :: tail if tag.runtimeClass == m.getClass =>
              ZIO.succeed((m.asInstanceOf[M], tail))
            case exp                                                  =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got receive[${tag.runtimeClass}]()"))
          }
        }

        def receive[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Message] = {
          expectationsRef.modify {
            case Receive(m) :: tail if tag1.runtimeClass == m.getClass || tag2.runtimeClass == m.getClass =>
              ZIO.succeed(m, tail)
            case exp                                                                                      =>
              ZIO.fail(
                new Exception(s"Expected ${exp.head}, got receive[${tag1.runtimeClass}, ${tag2.runtimeClass}]()")
              )
          }
        }

        def poll[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]] = {
          expectationsRef.modify {
            case Poll(None) :: tail =>
              ZIO.succeed((None.asInstanceOf[Option[M]], tail))

            case exp                =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got poll[${tag.runtimeClass}]()"))
          }
        }

        def ignore[M <: Message](implicit tag: ClassTag[M]): Task[Unit] = ???

        def unignore[M <: Message](implicit tag: ClassTag[M]): Task[Unit] = ???

        def poll[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Option[Message]] = ???

        def pollLast[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]] = ???

        def pollLast[M1, M2 <: Message](implicit
            tag1: ClassTag[M1],
            tag2: ClassTag[M2]
        ): Task[Option[Message]] = ???

        def peerId: PeerId = mockPeerId

        def peerIdStr: String = new String(mockPeerId.toArray, StandardCharsets.US_ASCII)

        def onMessage(msg: Message): Task[Unit] = ???
        private[peerwire] def sendActor         = ???
        private[peerwire] def receiveActor      = ???

        def waitForPeerInterested: Task[Unit]                                                    = ???
        def receiveWhilePeerInterested[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]] = ???
        def waitForPeerUnchoking: Task[Unit]                                                     = ???
        def receiveWhilePeerUnchoking[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]]  = ???
      }

      (dispatcher, peerHandle)
    }
  }
}
