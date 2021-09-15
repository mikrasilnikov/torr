package torr.peerwire

import java.time.OffsetDateTime
import scala.collection.mutable
import scala.reflect.ClassTag

class PeerMailbox {

  private val byMessageTag = new mutable.HashMap[ClassTag[_], mutable.Queue[(OffsetDateTime, Message)]]()

  def enqueue[M <: Message](received: OffsetDateTime, message: M)(implicit tag: ClassTag[M]): Unit = {
    byMessageTag.get(tag) match {
      case None        =>
        val queue = mutable.Queue.empty[(OffsetDateTime, Message)]
        queue.enqueue((received, message))
        byMessageTag.put(tag, queue)

      case Some(queue) =>
        queue.enqueue((received, message))
    }
  }

  /**
    * Dequeues the oldest received message having one of the specified types.
    */
  def dequeue(tags: List[ClassTag[_]]): Option[Message] = {

    val matchingNonEmptyQueues = tags
      .map(tag => (tag, byMessageTag.get(tag)))
      .filter { case (_, queueOption) => queueOption.isDefined }
      .map { case (tag, queueOption) => (tag, queueOption.get) }
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
    byMessageTag
      .filter { case (_, queue) => queue.nonEmpty }
      .map { case (_, queue) => queue.head }
      .minByOption { case (time, _) => time }
  }

  def size: Int = byMessageTag.foldLeft(0) { case (acc, (_, queue)) => acc + queue.size }

  def formatStats: String = {
    byMessageTag
      .toList
      .filter { case (_, queue) => queue.nonEmpty }
      .map { case (tag, queue) => (tag, queue.size) }
      .sortBy { case (_, size) => size }
      .reverse
      .map { case (tag, size) => s"$tag => $size" }
      .mkString("\n")
  }
}
