package torr.peerwire.test

import torr.dispatcher.{AcquireJobResult, Dispatcher, PeerId, PieceId, ReleaseJobStatus}
import torr.peerwire.{Message, PeerHandle}
import zio._
import zio.nio.core.Buffer

import java.nio.charset.StandardCharsets
import scala.reflect.ClassTag

object PeerHandleAndDispatcherMock {

  sealed trait Expectation[+_]
  case class Send(msg: Message)                                            extends Expectation[Unit]
  case class Receive(msg: Message)                                         extends Expectation[Message]
  case class ReceiveBlock(pieceId: Int, offset: Int, size: Int)            extends Expectation[Message]
  case class Poll(result: Option[Message])                                 extends Expectation[Option[Message]]
  case class IsDownloadCompleted(result: Boolean)                          extends Expectation[Boolean]
  case class IsRemoteInteresting(peerId: PeerId, result: Boolean)          extends Expectation[Boolean]
  case class AcquireJob(peerId: PeerId, result: AcquireJobResult)          extends Expectation[AcquireJobResult]
  case class ReleaseJob(peerId: PeerId, releaseStatus: ReleaseJobStatus)   extends Expectation[Unit]
  case class Fork(left: List[Expectation[_]], right: List[Expectation[_]]) extends Expectation[Unit]

  def makeMocks(mockPeerId: PeerId, expectations: List[Expectation[_]]): UIO[(Dispatcher.Service, PeerHandle)] = {

    def removeEmptyFork(expectations: List[Expectation[_]]): List[Expectation[_]] =
      expectations match {
        case Fork(Nil, Nil) :: tail => tail
        case _                      => expectations
      }

    RefM.make(expectations).map { expectationsRef =>
      val dispatcher = new Dispatcher.Service {

        def isDownloadCompleted: Task[Boolean] = {
          expectationsRef.map(removeEmptyFork).modify {
            case Fork(IsDownloadCompleted(res) :: tl, right) :: tail => ZIO.succeed(res, Fork(tl, right) :: tail)
            case Fork(left, IsDownloadCompleted(res) :: tr) :: tail  => ZIO.succeed(res, Fork(left, tr) :: tail)
            case IsDownloadCompleted(res) :: tail                    => ZIO.succeed(res, tail)
            case exp                                                 =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got isDownloadCompleted()"))
          }
        }

        def isRemoteInteresting(peerId: PeerId): Task[Boolean] = {
          expectationsRef.map(removeEmptyFork).modify {
            case Fork(IsRemoteInteresting(pid: PeerId, res) :: tl, right) :: tail if (peerId == pid) =>
              ZIO.succeed(res, Fork(tl, right) :: tail)
            case Fork(left, IsRemoteInteresting(pid, res) :: tr) :: tail if (peerId == pid)          =>
              ZIO.succeed(res, Fork(left, tr) :: tail)
            case IsRemoteInteresting(pid, res) :: tail if (peerId == pid)                            =>
              ZIO.succeed(res, tail)
            case exp                                                                                 =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got isRemoteInteresting($peerId)"))
          }
        }

        def acquireJob(peerId: PeerId): Task[AcquireJobResult] = {
          //ZIO(println(s"-- acquireJob($peerId, $remoteHave)")) *>
          expectationsRef.map(removeEmptyFork).modify {
            case Fork(AcquireJob(pid, res) :: tl, right) :: tail if peerId == pid =>
              ZIO.succeed(res, Fork(tl, right) :: tail)
            case Fork(left, AcquireJob(pid, res) :: tr) :: tail if peerId == pid  =>
              ZIO.succeed(res, Fork(left, tr) :: tail)
            case AcquireJob(pid, res) :: tail if peerId == pid                    =>
              ZIO.succeed(res, tail)
            case exp                                                              =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got acquireJob($peerId)"))
          }
        }

        def releaseJob(peerId: PeerId, releaseStatus: => ReleaseJobStatus): Task[Unit] =
          //ZIO(println(s"-- releaseJob($peerId, $releaseStatus)")) *>
          expectationsRef.map(removeEmptyFork).modify {
            case Fork(ReleaseJob(pid, rs) :: tl, right) :: tail if peerId == pid && releaseStatus == rs =>
              ZIO.succeed((), Fork(tl, right) :: tail)
            case Fork(left, ReleaseJob(pid, rs) :: tr) :: tail if peerId == pid && releaseStatus == rs  =>
              ZIO.succeed((), Fork(left, tr) :: tail)
            case ReleaseJob(pid, rs) :: tail if peerId == pid && releaseStatus == rs                    => ZIO.succeed((), tail)
            case exp                                                                                    =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got releaseJob($peerId, $releaseStatus)"))
          }

        def registerPeer(peerId: PeerId): Task[Unit]   = ???
        def unregisterPeer(peerId: PeerId): Task[Unit] = ???

        def reportHave(peerId: PeerId, piece: PieceId): Task[Unit]           = ???
        def reportHaveMany(peerId: PeerId, pieces: Set[PieceId]): Task[Unit] = ???

        def reportDownloadSpeed(peerId: PeerId, bytesPerSecond: PieceId): Task[Unit] = ???
        def reportUploadSpeed(peerId: PeerId, bytesPerSecond: PieceId): Task[Unit]   = ???
      }

      val peerHandle = new PeerHandle {

        def send(msg: Message): Task[Unit] = {
          //ZIO(println(s"-> $msg")) *>
          expectationsRef.map(removeEmptyFork).modify {
            case Fork(Send(m) :: tl, right) :: tail if msg == m => ZIO.succeed((), Fork(tl, right) :: tail)
            case Fork(left, Send(m) :: tr) :: tail if msg == m  => ZIO.succeed((), Fork(left, tr) :: tail)
            case Send(m) :: tail if msg == m                    => ZIO.succeed((), tail)
            case exp                                            =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got send($msg)"))
          }
        }

        def receive[M <: Message](implicit tag: ClassTag[M]): Task[M] = {
          //ZIO(println(s"<- ${tag.runtimeClass}")) *>
          expectationsRef.map(removeEmptyFork).modify {
            case Fork(ReceiveBlock(p, o, s) :: tl, right) :: tail     =>
              Buffer.byte(s).map(b => (Message.Piece(p, o, b).asInstanceOf[M], Fork(tl, right) :: tail))
            case Fork(left, ReceiveBlock(p, o, s) :: tr) :: tail      =>
              Buffer.byte(s).map(b => (Message.Piece(p, o, b).asInstanceOf[M], Fork(left, tr) :: tail))

            case Fork(Receive(m) :: tl, right) :: tail                =>
              ZIO.succeed(m.asInstanceOf[M], Fork(tl, right) :: tail)
            case Fork(left, Receive(m) :: tr) :: tail                 =>
              ZIO.succeed(m.asInstanceOf[M], Fork(left, tr) :: tail)

            case ReceiveBlock(p, o, s) :: tail                        =>
              Buffer.byte(s).map(b => (Message.Piece(p, o, b).asInstanceOf[M], tail))
            case Receive(m) :: tail if tag.runtimeClass == m.getClass =>
              ZIO.succeed((m.asInstanceOf[M], tail))
            case exp                                                  =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got receive[${tag.runtimeClass}]()"))
          }
        }

        def receive[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Message] = {
          //ZIO(println(s"<- ${tag1.runtimeClass} | ${tag2.runtimeClass}")) *>
          expectationsRef.map(removeEmptyFork).modify {
            case Fork(ReceiveBlock(p, o, s) :: tl, right) :: tail                                         =>
              Buffer.byte(s).map(b => (Message.Piece(p, o, b), Fork(tl, right) :: tail))
            case Fork(left, ReceiveBlock(p, o, s) :: tr) :: tail                                          =>
              Buffer.byte(s).map(b => (Message.Piece(p, o, b), Fork(left, tr) :: tail))
            case ReceiveBlock(p, o, s) :: tail                                                            =>
              Buffer.byte(s).map(b => (Message.Piece(p, o, b), tail))

            case Fork(Receive(m) :: tl, right) :: tail
                if tag1.runtimeClass == m.getClass || tag2.runtimeClass == m.getClass =>
              ZIO.succeed(m, Fork(tl, right) :: tail)
            case Fork(left, Receive(m) :: tr) :: tail
                if tag1.runtimeClass == m.getClass || tag2.runtimeClass == m.getClass =>
              ZIO.succeed(m, Fork(left, tr) :: tail)

            case Receive(m) :: tail if tag1.runtimeClass == m.getClass || tag2.runtimeClass == m.getClass =>
              ZIO.succeed(m, tail)
            case exp                                                                                      =>
              ZIO.fail(
                new Exception(s"Expected ${exp.head}, got receive[${tag1.runtimeClass}, ${tag2.runtimeClass}]()")
              )
          }
        }

        def poll[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]] = {
          //ZIO(println(s"<? ${tag.runtimeClass}")) *>
          expectationsRef.map(removeEmptyFork).modify {

            case Fork(Poll(Some(m)) :: tl, right) :: tail if tag.runtimeClass == m.getClass =>
              ZIO.succeed(Some(m).asInstanceOf[Option[M]], Fork(tl, right) :: tail)
            case Fork(left, Poll(Some(m)) :: tr) :: tail if tag.runtimeClass == m.getClass  =>
              ZIO.succeed(Some(m).asInstanceOf[Option[M]], Fork(left, tr) :: tail)
            case Poll(Some(m)) :: tail if tag.runtimeClass == m.getClass                    =>
              ZIO.succeed((Some(m).asInstanceOf[Option[M]], tail))

            case Fork(Poll(None) :: tl, right) :: tail                                      =>
              ZIO.succeed(None.asInstanceOf[Option[M]], Fork(tl, right) :: tail)
            case Fork(left, Poll(None) :: tr) :: tail                                       =>
              ZIO.succeed(None.asInstanceOf[Option[M]], Fork(left, tr) :: tail)
            case Poll(None) :: tail                                                         =>
              ZIO.succeed((None.asInstanceOf[Option[M]], tail))

            case exp                                                                        =>
              ZIO.fail(new Exception(s"Expected ${exp.head}, got poll[${tag.runtimeClass}]()"))
          }
        }

        def poll[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Option[Message]] = ???

        def peerId: PeerId = mockPeerId

        def peerIdStr: String = new String(mockPeerId.toArray, StandardCharsets.US_ASCII)

        def onMessage(msg: Message): Task[Unit] = ???
        private[peerwire] def sendActor         = ???
        private[peerwire] def receiveActor      = ???
      }

      (dispatcher, peerHandle)
    }
  }
}
