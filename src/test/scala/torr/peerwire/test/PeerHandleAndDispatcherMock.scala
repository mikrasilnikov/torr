package torr.peerwire.test

import torr.dispatcher.{AcquireJobResult, Dispatcher, PeerId, PieceId, ReleaseJobStatus}
import torr.peerwire.{Message, PeerHandle}
import zio._
import zio.nio.core.Buffer

import scala.reflect.ClassTag

object PeerHandleAndDispatcherMock {

  sealed trait Expectation[+_]
  case class Send(msg: Message)                                             extends Expectation[Unit]
  case class Receive(msg: Message)                                          extends Expectation[Message]
  case class ReceiveBlock(pieceId: Int, offset: Int, size: Int)             extends Expectation[Message]
  case class Poll(result: Option[Message])                                  extends Expectation[Option[Message]]
  case class IsDownloadCompleted(result: Boolean)                           extends Expectation[Boolean]
  case class IsRemoteInteresting(remoteHave: Set[PieceId], result: Boolean) extends Expectation[Boolean]
  case class AcquireJob(peerId: PeerId, remoteHave: Set[PieceId], result: AcquireJobResult)
      extends Expectation[AcquireJobResult]
  case class ReleaseJob(peerId: PeerId, releaseStatus: ReleaseJobStatus)    extends Expectation[Unit]
  case class Fork(left: List[Expectation[_]], right: List[Expectation[_]])  extends Expectation[Unit]

  def makeMocks(mockPeerId: PeerId, expectations: List[Expectation[_]]): UIO[(Dispatcher.Service, PeerHandle)] = {
    RefM.make(expectations).map { expectationsRef =>
      val dispatcher = new Dispatcher.Service {

        def isDownloadCompleted: Task[Boolean] = {
          expectationsRef.modify {
            case IsDownloadCompleted(res) :: tail => ZIO.succeed(res, tail)
            case exp                              =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got isDownloadCompleted()"))
          }
        }

        def isRemoteInteresting(remoteHave: Set[PieceId]): Task[Boolean] = {
          expectationsRef.modify {
            case IsRemoteInteresting(rh: Set[PieceId], res) :: tail if (remoteHave == rh) => ZIO.succeed(res, tail)
            case exp                                                                      =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got isRemoteInteresting($remoteHave)"))
          }
        }

        def acquireJob(peerId: PeerId, remoteHave: Set[PieceId]): Task[AcquireJobResult] = {
          ZIO(println(s"-- acquireJob($peerId, $remoteHave)")) *>
            expectationsRef.modify {
              case AcquireJob(pid, rh, res) :: tail if peerId == pid && remoteHave == rh => ZIO.succeed(res, tail)
              case exp                                                                   =>
                ZIO.fail(new Exception(s"Expected ${exp.head}, got acquireJob($peerId, $remoteHave)"))
            }
        }

        def releaseJob(peerId: PeerId, releaseStatus: => ReleaseJobStatus): Task[Unit] =
          ZIO(println(s"-- releaseJob($peerId, $releaseStatus)")) *>
            expectationsRef.modify {
              case ReleaseJob(pid, rs) :: tail if peerId == pid && releaseStatus == rs => ZIO.succeed((), tail)
              case exp                                                                 =>
                ZIO.fail(new Exception(s"Expected ${exp.head}, got releaseJob($peerId, $releaseStatus)"))
            }

        def registerPeer(peerId: PeerId): Task[Unit]   = ???
        def unregisterPeer(peerId: PeerId): Task[Unit] = ???
      }

      val peerHandle = new PeerHandle {

        def send(msg: Message): Task[Unit] = {
          ZIO(println(s"-> $msg")) *>
            expectationsRef.modify {
              case Send(m) :: tail if msg == m => ZIO.succeed((), tail)
              case exp                         =>
                ZIO.fail(new Exception(s"Expected ${exp.head}, got send($msg)"))
            }
        }

        def receive[M <: Message](implicit tag: ClassTag[M]): Task[M] = {
          ZIO(println(s"<- ${tag.runtimeClass}")) *>
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
          ZIO(println(s"<- ${tag1.runtimeClass} | ${tag2.runtimeClass}")) *>
            expectationsRef.modify {
              case ReceiveBlock(p, o, s) :: tail                                                            =>
                Buffer.byte(s).map(b => (Message.Piece(p, o, b), tail))
              case Receive(m) :: tail if tag1.runtimeClass == m.getClass || tag2.runtimeClass == m.getClass =>
                ZIO.succeed(m, tail)
              case exp                                                                                      =>
                ZIO.fail(
                  new Exception(s"Expected ${exp.head}, got receive[${tag1.runtimeClass}, ${tag2.runtimeClass}]()")
                )
            }
        }

        def poll[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]] = {
          ZIO(println(s"<? ${tag.runtimeClass}")) *>
            expectationsRef.modify {
              case Poll(Some(m)) :: tail if tag.runtimeClass == m.getClass =>
                ZIO.succeed((Some(m).asInstanceOf[Option[M]], tail))
              case Poll(None) :: tail                                      =>
                ZIO.succeed((None.asInstanceOf[Option[M]], tail))
              case exp                                                     =>
                ZIO.fail(new Exception(s"Expected ${exp.head}, got poll[${tag.runtimeClass}]()"))
            }
        }

        def peerId: PeerId = mockPeerId

        def onMessage(msg: Message): Task[Unit] = ???
        private[peerwire] def sendActor         = ???
        private[peerwire] def receiveActor      = ???
      }

      (dispatcher, peerHandle)
    }
  }
}
