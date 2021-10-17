package torr.peerwire

import zio._
import zio.actors.ActorRef

import scala.reflect.ClassTag
import torr.dispatcher.PeerId
import zio.logging.Logging

trait PeerHandle {
  def peerId: PeerId
  def peerIdStr: String
  def send(msg: Message): Task[Unit]
  def receive[M <: Message](implicit tag: ClassTag[M]): Task[M]
  def receive[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Message]

  def waitForPeerInterested: Task[Unit]
  def receiveWhilePeerInterested[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]]
  def waitForPeerUnchoking: Task[Unit]
  def receiveWhilePeerUnchoking[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]]

  def pollLast[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]]
  def pollLast[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Option[Message]]
  def poll[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]]
  def poll[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Option[Message]]
  def ignore[M <: Message](implicit tag: ClassTag[M]): Task[Unit]
  def unignore[M <: Message](implicit tag: ClassTag[M]): Task[Unit]
  def log(message: String): RIO[Logging, Unit] = Logging.debug(s"$peerIdStr $message")

  def onMessage(msg: Message): Task[Unit]

  private[peerwire] def sendActor: ActorRef[SendActor.Command]
  private[peerwire] def receiveActor: ActorRef[ReceiveActor.Command]
}
