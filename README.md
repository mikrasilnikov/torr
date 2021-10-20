###Overview

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

Building
- zio-cli

Architecture
- actors and mutable state
- disk cache
- peer routines