package torr

import zio._
import zio.macros.accessible

package object actorsystem {
  type ActorSystem = Has[ActorSystem.Service]

  @accessible
  object ActorSystem {
    trait Service {
      def system: zio.actors.ActorSystem
    }
  }
}
