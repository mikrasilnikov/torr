package torr.peerwire

import torr.channels.ByteChannel
import torr.directbuffers.DirectBufferPool
import torr.peerwire.Message.Piece
import zio._
import zio.actors._
import zio.actors.Actor.Stateful
import zio.logging.Logging
import zio.nio.core.ByteBuffer

object SendActor {

  sealed trait Command[+_]
  case class Send(message: Message) extends Command[Unit]

  case class State(
      channel: ByteChannel,
      remotePeerIdStr: String,
      sendBuf: ByteBuffer,
      receiveActor: ActorRef[ReceiveActor.Command]
  )

  val stateful = new Stateful[DirectBufferPool with Logging, State, Command] {

    def receive[A](state: State, cmd: Command[A], context: Context): RIO[DirectBufferPool with Logging, (State, A)] = {
      cmd match {
        case Send(msg) => send(state, msg).as(state, ())
      }
    }
  }

  private[peerwire] def send(
      state: State,
      msg: Message
  ): RIO[DirectBufferPool with Logging, Unit] = {
    val effect =
      msg match {
        case Piece(_, _, data) =>
          Message.send(msg, state.channel, state.sendBuf) *>
            DirectBufferPool.free(data)
        case _                 =>
          Message.send(msg, state.channel, state.sendBuf)
      }

    //Logging.debug(s"${state.remotePeerIdStr} -> $msg") *>
    effect
      .foldM(
        e => (state.receiveActor ! ReceiveActor.StartFailing(e)).orDie,
        _ => ZIO.unit
      )
  }
}
