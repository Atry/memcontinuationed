Memcontinuationed
=================

**Memcontinuationed** is an asynchronous memcached client for Scala. Memcontinuationed is the fastest memcached client
on JVM, much faster than [spymemcached](https://code.google.com/p/spymemcached/) and
[Whalin's client](http://www.whalin.com/memcached).

## Why is Memcontinuationed so fast?

### Reason 1. Better threading model

Memcontinuationed never blocks any threads. On the other hand, spymemcached does not block the IO thread but it will
block the user's thread. All Memcontinuationed API is
[suspendable](http://www.scala-lang.org/api/current/scala/util/continuations/package.html#suspendable=scala.util.continuations.package.cps%5BUnit%5D),
which mean these methods can be invoked by a thread, and return to another thread.

### Reason 2. Optimization for huge number of IOPS

Memcontinuationed can merge multiply `get` or `gets` requests into one request.
On the other hand, spymemcached sends all requests immediately, never waiting for previous response.
The spymemcached way consumes more CPU and more TCP overheads than Memcontinuationed.
Even worse, the spymemcached way is not compatible with [some memcached server](http://wiki.open.qq.com/wiki/%E8%AE%BF%E9%97%AECMEM).

Note: Because Memcontinuationed does not send requests until it receives previous reponse,
you may need to creating a connection pool of `com.dongxiguo.memcontinuationed.Memcontinuationed` to maximize the IOPS.

## A sample to use Memcontinuationed

    import com.dongxiguo.memcontinuationed.Memcontinuationed
    import com.dongxiguo.memcontinuationed.StorageAccessor
    import java.io._
    import java.net._
    import java.nio.channels.AsynchronousChannelGroup
    import java.util.concurrent.Executors
    import scala.util.continuations.reset
    import scala.util.control.Exception.Catcher
    
    object Sample {
    
      def main(args: Array[String]) {
        val threadPool = Executors.newCachedThreadPool()
        val channelGroup = AsynchronousChannelGroup.withThreadPool(threadPool)
    
        // The locator decide where the memcached server is.
        // You may want to implement ketama hashing here.
        def locator(accessor: StorageAccessor[_]) = {
          new InetSocketAddress("localhost", 1978)
        }
    
        val memcontinuationed = new Memcontinuationed(channelGroup, locator)
    
        // The error handler
        implicit def catcher:Catcher[Unit] = {
          case e: Exception =>
            scala.Console.err.print(e)
            sys.exit(-1)
        }
    
        reset {
          memcontinuationed.set(MyKey("hello"), "Hello, World!")
          val result = memcontinuationed.get(MyKey("hello"))
          assert(result == "Hello, World!")
          println(result)
          sys.exit()
        }
      }
    }
    
    /**
     * `MyKey` specifies how to serialize the data of key/value pair.
     */
    case class MyKey(override val key: String) extends StorageAccessor[String] {
    
      override def encode(output: OutputStream, data: String, flags: Int) {
        output.write(data.getBytes("UTF-8"))
      }
    
      override def decode(input: InputStream, flags: Int): String = {
        val result = new Array[Byte](input.available)
        input.read(result)
        new String(result, "UTF-8")
      }
    }

There is something you need to know.

* `get`, `set`, and most of other methods in `Memcontinuationed` is `@suspendable`. You must invoke them in `reset` or in another `@suspendable` function you defined.
* `get`, `set`, and most of other methods in `Memcontinuationed` accept an implicit parameter `Catcher`. You must use `Catcher` to handle exceptions from `@suspendable` functions, instead of `try`/`catcher`.
* `MyKey` is the key you passed to server, which is a custom `StorageAccessor`. You should implement your own `StorageAccessor` for each type of data.

The API documentation can be download at http://central.maven.org/maven2/com/dongxiguo/memcontinuationed_2.10/0.3.0/memcontinuationed_2.10-0.3.0-javadoc.jar<wbr/>.
See https://github.com/Atry/memcontinuationed/tree/master/src/test/scala/com/dongxiguo/memcontinuationed for more example.

## Build configuration

Add these lines to your `build.sbt` if you are using [Sbt](http://www.scala-sbt.org/).

    libraryDependencies += "com.dongxiguo" %% "memcontinuationed" % "0.3.0"
    
    scalaVersion := "2.10.0"

See http://mvnrepository.com/artifact/com.dongxiguo/memcontinuationed_2.10/0.3.0 if you use [Maven](http://maven.apache.org/)
or other build systems.

## Requirement

Memcontinuationed requires Scala 2.10.x and JRE 7.