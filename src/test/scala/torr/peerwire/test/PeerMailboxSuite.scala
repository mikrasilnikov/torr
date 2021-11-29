package torr.peerwire.test

import torr.peerwire.{Message, PeerMailbox}
import zio._
import zio.test._
import zio.test.Assertion._

import java.time.{OffsetDateTime, ZoneOffset}

object PeerMailboxSuite extends DefaultRunnableSpec {
  override def spec =
    suite("PeerMailboxSuite")(
      //
      test("Dequeue on empty mailbox") {
        val mailbox = new PeerMailbox
        val actual  = mailbox.dequeue1(classOf[Message.KeepAlive.type])
        assert(actual)(isNone)
      },
      //
      test("Size of empty mailbox") {
        val mailbox = new PeerMailbox
        val actual  = mailbox.size
        assert(actual)(equalTo(0))
      },
      //
      test("Enqueues and dequeues one message") {
        val mailbox  = new PeerMailbox
        val expected = Message.KeepAlive
        mailbox.enqueue(origin, Message.KeepAlive)
        val size1    = mailbox.size
        val actual   = mailbox.dequeue1(classOf[Message.KeepAlive.type])
        val size0    = mailbox.size

        assert(actual)(isSome(equalTo(expected))) &&
        assert(size1)(equalTo(1)) &&
        assert(size0)(equalTo(0))
      },
      //
      test("Enqueues and dequeues two messages") {
        val mailbox = new PeerMailbox
        val msg1    = Message.Have(1)
        val msg2    = Message.Have(2)
        mailbox.enqueue(origin.plusSeconds(0), msg1)
        mailbox.enqueue(origin.plusSeconds(1), msg2)
        val size2   = mailbox.size
        val actual  = mailbox.dequeue1(classOf[Message.Have])
        val size1   = mailbox.size

        assert(actual)(isSome(equalTo(msg1))) &&
        assert(size1)(equalTo(1)) &&
        assert(size2)(equalTo(2))
      },
      //
      test("Dequeues message of correct type") {
        val mailbox = new PeerMailbox
        val msg1    = Message.Have(1)
        val msg2    = Message.Port(12345)
        mailbox.enqueue(origin.plusSeconds(0), msg1)
        mailbox.enqueue(origin.plusSeconds(0), msg2)
        val actual  = mailbox.dequeue1(classOf[Message.Port])
        assert(actual)(isSome(equalTo(msg2)))
      },
      //
      test("Dequeues message of correct type - no such message") {
        val mailbox = new PeerMailbox
        val msg1    = Message.Have(1)
        val msg2    = Message.Port(12345)
        mailbox.enqueue(origin.plusSeconds(0), msg1)
        mailbox.enqueue(origin.plusSeconds(0), msg2)
        val actual  = mailbox.dequeue1(classOf[Message.KeepAlive.type])
        assert(actual)(isNone)
      },
      //
      test("Returns dateTime of the oldest message - empty queue") {
        val mailbox = new PeerMailbox
        val actual  = mailbox.oldestMessage
        assert(actual)(isNone)
      },
      //
      test("Returns dateTime of the oldest message") {
        val mailbox = new PeerMailbox
        val msg1    = Message.Have(1)
        val msg2    = Message.Port(12345)
        mailbox.enqueue(origin.plusSeconds(10), msg1)
        mailbox.enqueue(origin.plusSeconds(20), msg2)
        val actual  = mailbox.oldestMessage
        assert(actual)(isSome(equalTo(origin.plusSeconds(10), msg1)))
      }
    )

  private val origin =
    OffsetDateTime.of(
      2021,
      1,
      1,
      0,
      0,
      0,
      0,
      ZoneOffset.UTC
    )
}
