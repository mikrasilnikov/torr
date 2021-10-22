## Overview

Torr is a Bittorrent client written in Scala using [ZIO](https://zio.dev/). The goal of this project for me 
was to learn Scala and explore the possibilities that modern functional programming can offer for 
developing high performance applications with a lot of concurrency under the hood.  

[Bittorrent protocol](https://wiki.theory.org/BitTorrentSpecification) is not a very hard thing to implement.
However, making a fast and efficient client is quite tricky. The performance target was to be at least 
comparable with uTorrent by download and upload speeds. Sure there is no way a ZIO application on top of 
the JVM can compete with native binary in CPU efficiency. The way to solve this problem is to make 
application scalable across multiple CPU cores. As it turned out, this approach lead to twice the throughput
of uTorrent.  

Torr implements only basic features of Bittorrent protocol. It does not support [DHT](http://bittorrent.org/beps/bep_0005.html), 
[PEX](http://bittorrent.org/beps/bep_0011.html) and other useful extensions. Implementing these goes 
out of scope of this experimental hobby project. Therefore, it is not supposed to replace any of existing 
clients that are able to find peers without a tracker and feature internal bandwidth management.  

The project has been completed. The client works. Here is how it looks in action:

## Building

Torr depends on [zio-cli](https://github.com/zio/zio-cli) to parse command line arguments. This library 
has not been published to Maven yet, so it is necessary to clone the repository and `sbt publishLocal` it.

## Usage

## Architecture

### Actors and mutable state

Torr internally consists of a bunch of services that support asynchronous operations. For example, a resource
pool may provide `acquire` and `release` methods. Client code must wait for the result of `acquire` but there
is no need to block until `release` is completed. Therefore, the `release` operation should execute asynchronously.

One possible implementation of this kind of [service](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/directbuffers/GrowableBufferPool.scala) would be an [actor](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/directbuffers/GrowablePoolActor.scala) and [wrapper](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/directbuffers/GrowableBufferPool.scala) for it. If a client must 
wait for the result of an operation, wrapper would send the corresponding message with `?` (ask operator). 
If an operation may be executed asynchronously, wrapper would use `!` (tell).

Torr does exploit this pattern a lot. This led to two consequences that should be discussed:

- Actors provide a convenient way to wrap mutable state. And [such state](https://github.com/mikrasilnikov/torr/blob/main/src/main/scala/torr/peerwire/ReceiveActorState.scala) 
does not look like a piece of a functional codebase. However, I decided not to use immutable data structures
for such type of state because while it may make the code look a little nicer, it would also waste resources
by putting more pressure on the GC.
- 

### Disk cache

### Peer routines

Benchmarks
