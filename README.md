Memcontinuationed
=================

**Memcontinuationed** is an asynchronous memcached client for Scala. Memcontinuationed is the fastest memcached client
on JVM, much faster than [spymemcached](https://code.google.com/p/spymemcached/) and
[Whalin's client](http://www.whalin.com/memcached).

## Why is Memcontinuationed so fast?

### Reason 1: Better threading model

Memcontinuationed never blocks any threads. On the other hand, spymemcached does not block the IO thread but it will
block the user's thread. All Memcontinuationed API is
[suspendable](http://www.scala-lang.org/api/current/scala/util/continuations/package.html#suspendable=scala.util.continuations.package.cps%5BUnit%5D),
which mean these methods can be invoked by a thread, and return to another thread.

### Reason 2: Optimization for huge number of IOPS

Memcontinuationed can merge multiply `get` or `gets` requests into one request.
On the other hand, spymemcached sends all requests immediately, never waiting for previous response.
The spymemcached way consumes more CPU and more TCP overheads than Memcontinuationed.
Even worse, the spymemcached way is not compatible with [some memcached server](http://wiki.open.qq.com/wiki/%E8%AE%BF%E9%97%AECMEM).

Note: Because Memcontinuationed does not send requests until it receives previous reponse,
you may need to creating a connection pool of `com.dongxiguo.memcontinuationed.Memcontinuationed` to maximize the IOPS.
