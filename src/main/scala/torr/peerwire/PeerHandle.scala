package torr.peerwire

import zio.Task
import zio.actors.ActorRef
import scala.reflect.ClassTag
import torr.dispatcher.PeerId

trait PeerHandle {
  def peerId: PeerId
  def send(msg: Message): Task[Unit]
  def receive[M <: Message](implicit tag: ClassTag[M]): Task[M]
  def receive[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Message]
  def poll[M <: Message](implicit tag: ClassTag[M]): Task[Option[M]]
  def poll[M1, M2 <: Message](implicit tag1: ClassTag[M1], tag2: ClassTag[M2]): Task[Option[Message]]
  def onMessage(msg: Message): Task[Unit]

  private[peerwire] def sendActor: ActorRef[SendActor.Command]
  private[peerwire] def receiveActor: ActorRef[ReceiveActor.Command]
}
