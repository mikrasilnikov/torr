import zio.Chunk

import java.math.BigInteger


val s = "2e000fa7e85759c7f4c254d4d9c33ef481e459a7"
s.grouped(2)
  .map(item => new BigInteger(item, 16).toByteArray)
  .map(Chunk.fromArray)
  .reduce(_ concat _)


