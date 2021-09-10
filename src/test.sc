import scala.collection.mutable
import scala.reflect.ClassTag

sealed trait Message
case object KeepAlive extends Message
case class WithPayload(value : String) extends Message


val s = new mutable.HashMap[ClassTag[_], String]()
s += (ClassTag(KeepAlive.getClass) -> "Promise1")


