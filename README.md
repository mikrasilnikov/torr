## Overview

Torr is a Bittorrent client written in Scala using [ZIO](https://zio.dev/). The goal of this project for me 
was to learn Scala and explore the possibilities that modern functional programming can offer for 
developing high performance applications with a lot of concurrency under the hood.

[Bittorrent protocol](https://wiki.theory.org/BitTorrentSpecification) is not a very hard thing to implement.
However, making a fast and efficient client is quite tricky. The performance target was to be at least 
comparable with uTorrent by download and upload speeds. Sure there is no way a ZIO application on top of 
the JVM can compete with native binary in CPU efficiency. The way to solve this problem is to make 
application scalable across multiple CPU cores. As it turned out, this approach lead to speeds that are not
only comparable but surpassing uTorrent's performance (see benchmarks).

Torr implements only basic features of Bittorrent protocol. It does not support [DHT](http://bittorrent.org/beps/bep_0005.html), 
[PEX](http://bittorrent.org/beps/bep_0011.html), [uTP](https://www.bittorrent.org/beps/bep_0029.html) and other extensions. 
Implementing these goes out of scope of this project. Therefore, it is not supposed to 
replace any of the existing clients that are able to find peers without a tracker and feature internal bandwidth management.  

The project is now completed. The client works. Here is how it looks in action:

## Building

Torr depends on [zio-cli](https://github.com/zio/zio-cli) to parse command line arguments. This library 
has not been published to Maven yet, so it is necessary to clone the repository and `sbt publishLocal` it.

## Usage
```
java -jar torr.jar [--port listenPort] [--maxConn maxConnections] [--proxy proxyAddr] 
[--maxDown maxSimultaneousDownloads] [--maxUp maxSimultaneousUploads] 
torrentFile additionalPeer ...
```
Examples:

```
java -jar torr.jar ubuntu-21.04-desktop-amd64.iso.torrent
```

```
java -jar torr.jar --port 55123 --maxConn 500 --proxy 127.0.0.1:8080 --maxDown 20 --maxUp 20 
ubuntu-21.04-desktop-amd64.iso.torrent 217.111.45.01:54184 217.111.45.02:41265
```
As it turned out, zio-cli does not support command line parameters with non-ascii characters on
Windows.

## Implementation remarks

### Actors and mutable state

Torr internally consists of a bunch of services that support asynchronous operations. For example, a resource
pool may provide `acquire` and `release` methods. Client code must wait for the result of `acquire` but there
is no need to block until `release` is completed. Therefore, the `release` operation should execute asynchronously.

One possible implementation of this kind of [service](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/directbuffers/package.scala) would be an [actor](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/directbuffers/GrowablePoolActor.scala) 
and [wrapper](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/directbuffers/GrowableBufferPool.scala) 
for it. If a client must wait for the result of an operation, wrapper would send the corresponding message with 
`?` (ask operator). If an operation may be executed asynchronously, wrapper would use `!` (tell).

Torr does exploit this pattern a lot. This led to two consequences that should be discussed:

- Actors provide a convenient way to wrap mutable state. And [such state](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/peerwire/ReceiveActorState.scala) 
does not look like a part of a functional codebase. However, I decided not to use immutable data structures
for such type of state because while it may make the code look a little nicer, it would also waste resources
by putting more pressure on the GC.
- Torr does not employ most of the features of the [zio-actors](https://zio.github.io/zio-actors/) library 
like remoting and persistence. Thus depending on an actor framework in this case may seem like an overkill.
However, the [actual implementation](https://github.com/zio/zio-actors/blob/master/actors/src/main/scala/zio/actors/Actor.scala) 
of a local actor in zio-actors is very lightweight and looks like the thing that I would have been making anyway.

### Peer handles

When a connection to a remote peer is established, a peer handle is created. It is backed by a couple of actors
for sending and receiving messages and provides a fiber-safe [methods](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/peerwire/PeerHandle.scala) 
to exchange messages and query state.

This approach allows to divide the protocol to a number of aspects and implement them independently. For example,
if we are required to reply to KeepAlive messages, it is possible to spawn a separate fiber and pass a 
peer handle to it:

```scala
...
  aliveFib      <- handleKeepAlive(peerHandle).fork
...

  private def handleKeepAlive(peerHandle: PeerHandle): Task[Unit] = {
    for {
      _ <- peerHandle.receive[KeepAlive]
      _ <- peerHandle.send(Message.KeepAlive)
      _ <- handleKeepAlive(peerHandle)
    } yield ()
  }
```
If the example above seems too trivial, here is [the 100 lines of code that handle all upload requests](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/peerroutines/UploadRoutine.scala).  

And here is the [single function that handles the whole Bittorrent protocol](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/peerroutines/PeerRoutine.scala).

### Benchmarks

These results were measured for clients downloading a torrent (~20GB of size) that was seeded by qBittorrent 4.3.8
on a local machine (i5-6500). The amount of CPU time spent was reported by Windows Task Manager.

Client                                  | Sustained download speed          | CPU Time                    
---                                     | ---                               | ---  
qBitTorrent 4.3.8                       | &gt; 200 MB/s (IO bound)          | 01m 15s
Torr                                    | 88 MB/s (CPU bound)               | 10m 37s
uTorrent 3.2.3                          | 53 MB/s                           | 04m 03s
Vuse 5.7.6.0                            | 33 MB/s                           | 02m 58s                               
     
                  