package torr.peerwire

import torr.channels.ByteChannel
import zio.{Chunk, Promise}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Map, Set}

//noinspection ReferenceMustBePrefixed
case class ReceiveActorState(
    remotePeerId: Chunk[Byte] = Chunk.fill(20)(0.toByte),
    remotePeerIdStr: String = "default peerId",
    //
    expectedMessages: Map[Class[_], Promise[Throwable, Message]] =
      HashMap[Class[_], Promise[Throwable, Message]](),
    givenPromises: Map[Promise[Throwable, Message], List[Class[_]]] =
      HashMap[Promise[Throwable, Message], List[Class[_]]](),
    //
    ignoredMessages: Set[Class[_]] = HashSet[Class[_]](),
    //
    var peerInterested: Boolean = false,
    waitingForPeerInterested: ArrayBuffer[Promise[Throwable, Unit]] =
      ArrayBuffer[Promise[Throwable, Unit]](),
    expectedWhilePeerInterested: HashMap[Class[_], Promise[Throwable, Option[Message]]] =
      HashMap[Class[_], Promise[Throwable, Option[Message]]](),
    //
    var peerUnchoking: Boolean = false,
    waitingForPeerUnchoking: ArrayBuffer[Promise[Throwable, Unit]] =
      ArrayBuffer[Promise[Throwable, Unit]](),
    expectedWhilePeerUnchoking: HashMap[Class[_], Promise[Throwable, Option[Message]]] =
      HashMap[Class[_], Promise[Throwable, Option[Message]]](),
    //
    mailbox: PeerMailbox = new PeerMailbox,
    actorConfig: PeerActorConfig = PeerActorConfig.default,
    var isFailingWith: Option[Throwable] = None
) {
  def expectingMessage(cls: Class[_]): Boolean = {
    expectedMessages.contains(cls) || expectedWhilePeerInterested.contains(cls)
  }
}
