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
  def dequeueMany(classes: List[Class[_]]): Option[Message] = {

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

  def dequeue1(cls: Class[_]): Option[Message] = {
    byMessageClass.get(cls) match {
      case Some(q) => q.removeHeadOption().map(_._2)
      case None    => None
    }
  }

  def dequeue2(cls1: Class[_], cls2: Class[_]): Option[Message] = {
    val q1Option = byMessageClass.get(cls1)
    val q2Option = byMessageClass.get(cls2)

    if (q1Option.isDefined && q2Option.isDefined) {
      val q1          = q1Option.get
      val q2          = q2Option.get
      val head1Option = q1.headOption
      val head2Option = q2.headOption

      if (head1Option.isDefined && head2Option.isDefined) {
        val head1 = head1Option.get
        val head2 = head2Option.get

        if (head1._1.compareTo(head2._1) < 0)
          Some(q1.dequeue()._2)
        else
          Some(q2.dequeue()._2)

      } else if (head1Option.isDefined) {
        Some(q1.dequeue()._2)
      } else if (head2Option.isDefined) {
        Some(q2.dequeue()._2)
      } else {
        None
      }
    } else if (q1Option.isDefined) {
      q1Option.get.removeHeadOption().map(_._2)
    } else if (q2Option.isDefined) {
      q2Option.get.removeHeadOption().map(_._2)
    } else {
      None
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
