package torr.peerwire

import torr.channels.ByteChannel
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

  val stateful = new Stateful[Logging, State, Command] {

    def receive[A](state: State, cmd: Command[A], context: Context): RIO[Logging, (State, A)] = {
      cmd match {
        case Send(msg) => send(state, msg).as(state, ())
      }
    }
  }

  private[peerwire] def send(
      state: State,
      msg: Message
  ): RIO[Logging, Unit] = {
    Logging.debug(s"${state.remotePeerIdStr} -> $msg") *>
      Message.send(msg, state.channel, state.sendBuf)
        .foldM(
          e => (state.receiveActor ! ReceiveActor.StartFailing(e)).orDie,
          _ => ZIO.unit
        )
  }
}
