package torr.peerwire

import java.time.OffsetDateTime
import scala.collection.mutable

class PeerMailbox {

  private[peerwire] val byMessageClass = new mutable.HashMap[Class[_], mutable.Queue[(OffsetDateTime, Message)]]()

  def enqueue[M <: Message](received: OffsetDateTime, message: M): Unit = {
    val tag = message.getClass
    byMessageClass.get(tag) match {
      case None        =>
        val queue = mutable.Queue.empty[(OffsetDateTime, Message)]
        queue.enqueue((received, message))
        byMessageClass.put(tag, queue)

      case Some(queue) =>
        queue.enqueue((received, message))
    }
  }

  /**
    * Dequeues the oldest received message having one of the specified types.
    */
  def dequeue(classes: List[Class[_]]): Option[Message] = {

    val matchingNonEmptyQueues = classes
      .map(cls => (cls, byMessageClass.get(cls)))
      .filter { case (_, queueOption) => queueOption.isDefined }
      .map { case (cls, queueOption) => (cls, queueOption.get) }
      .filter { case (_, queue) => queue.nonEmpty }
      .sortBy { case (_, queue) => queue.head._1 }

    matchingNonEmptyQueues match {
      case Nil             => None
      case (_, queue) :: _ =>
        val (_, message) = queue.dequeue()
        Some(message)
    }
  }

  def oldestMessage: Option[(OffsetDateTime, Message)] = {
    byMessageClass
      .filter { case (_, queue) => queue.nonEmpty }
      .map { case (_, queue) => queue.head }
      .minByOption { case (time, _) => time }
  }

  def size: Int = byMessageClass.foldLeft(0) { case (acc, (_, queue)) => acc + queue.size }

  def formatStats: String = {
    byMessageClass
      .toList
      .filter { case (_, queue) => queue.nonEmpty }
      .map { case (cls, queue) => (cls, queue.size) }
      .sortBy { case (_, size) => size }
      .reverse
      .map { case (cls, size) => s"$cls => $size" }
      .mkString("\n")
  }
}
