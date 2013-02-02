/*
 * Copyright 2013 深圳市葡萄藤网络科技有限公司 (Shenzhen Putaoteng Network Technology Co., Ltd.)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dongxiguo.memcontinuationed

import java.util.Collections
import java.util.Arrays
import java.util.concurrent._
import java.util.concurrent.atomic._
import java.nio.channels._
import java.nio._
import java.io._
import java.net._
import scala.util.control.Exception.Catcher
import scala.util.continuations._
import scala.collection.JavaConverters._
import com.dongxiguo.commons.continuations.io._
import com.dongxiguo.commons.continuations._
import com.google.protobuf._
import com.dongxiguo.zeroLog.context.CurrentMethodName
import com.dongxiguo.zeroLog.context.CurrentSource
import com.dongxiguo.zeroLog.context.CurrentLine
import com.dongxiguo.zeroLog.context.CurrentClass
import com.dongxiguo.fastring.Fastring.Implicits._
import com.dongxiguo.fastring.Fastring

private[memcontinuationed] object AsciiProtocol {

  implicit private val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)

  private def raise(throwable: Throwable)(
    implicit catcher: Catcher[Unit],
    currentSource: CurrentSource,
    currentLine: CurrentLine,
    currentClassName: CurrentClass,
    currentMethodNameOption: Option[CurrentMethodName]): Nothing @suspendable = {
    logger.severe(throwable)
    if (catcher.isDefinedAt(throwable)) {
      catcher(throwable)
    } else {
      throw throwable
    }
    shift(Hang)
  }

  private final val MaxValueSize = 32768

  implicit private class LineReader(val inputStream: AsynchronousInputStream) {

    @throws(classOf[IOException])
    private[AsciiProtocol] final def readUntilCRLF(
      packetLimit: Int = MaxValueSize,
      out: ByteArrayOutputStream = new ByteArrayOutputStream)(
        implicit catcher: Catcher[Unit]): String @suspendable = {
      var rest = packetLimit
      var cr, lf: Int = 0
      while (lf != -1 && rest != 0 && (cr != '\r' || lf != '\n')) {
        inputStream.available = 1
        cr = lf
        lf = inputStream.read()
        out.write(lf)
        rest = rest - 1
      }
      if (cr == '\r' && lf == '\n') {
        out.toString("UTF-8")
      } else {
        if (rest == 0) {
          raise(new ProtocolException("Line is too long."))
        } else {
          assert(lf == -1)
          raise(
            new ProtocolException("Unexpected end of data from socket."))
        }
      }
    }
  }

  private val KeyRegex = """[^\p{Cntrl}\s]+""".r

  private val GetsLineRegex =
    """\s*VALUE\s+(\S+)\s+(\d+)\s+(\d+)\s+(\d+)\s*\r\n""".r

  /**
   * @return `_2`和`_3`中每一项都与`_1`中每一项一一对应。不存在的键值对在`_2`中为`null`
   */
  @throws(classOf[IOException])
  @throws(classOf[ProtocolException])
  final def gets[Value](
    socket: AsynchronousSocketChannel,
    accessors: StorageAccessor[Value]*)(
      implicit catcher: Catcher[Unit],
      m1: Manifest[Value],
      m2: Manifest[StorageAccessor[Value]]): (Array[StorageAccessor[Value]], Array[Value], Array[CasId]) @suspendable = {
    logger.fine {
      fast"Request ${accessors.mkFastring(",")} on $socket"
    }
    def loggedCatcher: Catcher[Unit] = {
      case e: IOException =>
        logger.severe(e)
        SuspendableException.catchOrThrow(e)
    }
    val outputStream = new ByteArrayOutputStream
    val writer = new OutputStreamWriter(outputStream, "UTF-8")
    writer.write("gets")
    for (accessor <- accessors) {
      val key @ KeyRegex() = accessor.key
      writer.write(' ')
      writer.write(key)
    }
    writer.write("\r\n")
    writer.flush()
    logger.finer {
      fast"Request buffer is full filled:\n${new String(outputStream.toByteArray, "UTF-8")}"
    }
    sendAndReceive[(Array[StorageAccessor[Value]], Array[Value], Array[CasId])](
      socket, Array(ByteBuffer.wrap(outputStream.toByteArray))) { inputStream =>
        val sortedAccessors = accessors.toArray
        Arrays.sort[StorageAccessor[Value]](sortedAccessors, StorageAccessorComparator)
        val values = Array.ofDim[Value](accessors.length)
        val casIds = Array.ofDim[CasId](accessors.length)

        logger.finer("Waiting for response...")
        // Workaround for https://issues.scala-lang.org/browse/SI-6579
        @inline def parseLine(line: String): Unit @suspendable = {
          line match {
            case GetsLineRegex(key, flags, numBytes, casId) if numBytes.toInt <= MaxValueSize =>
              logger.fine {
                fast"Receive a header for $key(${
                  if (flags == 0)
                    fast""
                  else
                    fast"flags=$flags,"
                }numBytes=$numBytes,casId=$casId)"
              }
              (inputStream.available = numBytes.toInt)(loggedCatcher)
              import language.postfixOps
              val index =
                Collections.binarySearch(
                  sortedAccessors.view map { _.key } asJava,
                  key)
              if (index < 0) {
                raise(
                  new ProtocolException("Receive an unexpected key: " + key))
              } else {
                val accessor = sortedAccessors(index)
                values(index) = accessor.decode(inputStream, flags.toInt)
                if (inputStream.available > 0) {
                  logger.info {
                    fast"Decoder for $accessor does not consume all data."
                  }
                  inputStream.skip(inputStream.available)
                }
                casIds(index) = new CasId(casId.toLong)
                (inputStream.available = 2)(loggedCatcher)
                if (inputStream.read() != '\r') {
                  raise(
                    new ProtocolException(
                      "There must be CRLF after binary data."))
                } else {
                  if (inputStream.read() != '\n') {
                    raise(
                      new ProtocolException(
                        "There must be CRLF after binary data."))
                  }
                }
              }
            case ServerErrorRegex(reason) =>
              raise(new ServerErrorException(reason))
            case _ =>
              raise(new ProtocolException("Bad line " + line))
          }
        }

        // Workaround for https://issues.scala-lang.org/browse/SI-6603
        var line: String = null
        line = inputStream.readUntilCRLF()
        while (line != "END\r\n") {
          parseLine(line)
          line = inputStream.readUntilCRLF()
        }
        logger.finer("Receive \"END\"")
        (sortedAccessors, values, casIds)
      }(loggedCatcher)
  }

  private def sendAndReceive[A](
    s: AsynchronousSocketChannel,
    request: Array[ByteBuffer])(
      responseHandler: AsynchronousInputStream => A @suspendable)(
        implicit catcher: Catcher[Unit]): A @suspendable = {
    shift(
      new AtomicInteger(2) with ((A => Unit) => Unit) {
        var a: Any = null
        override final def apply(continue: A => Unit) {
          reset {
            logger.finest("Send request...")
            writeAll(s, request, 1L, TimeUnit.SECONDS)
            logger.finest("Request is sent.")
            if (decrementAndGet() == 0) {
              continue(a.asInstanceOf[A])
            }
          }
          reset {
            a = responseHandler(
              new AsynchronousInputStream {
                override val socket = s
                override def readingTimeout = 3L
                override def readingTimeoutUnit = TimeUnit.SECONDS
              })
            if (decrementAndGet() == 0) {
              continue(a.asInstanceOf[A])
            }
          }
        }
      })
  }

  private val GetLineRegex =
    """\s*VALUE\s+(\S+)\s+(\d+)\s+(\d+)(?:\s+\d*)?\r\n""".r

  /**
   * @return `_2`中每一项都与`_1`中每一项一一对应。不存在的键值对在`_2`中为`null`
   */
  final def get[Value](
    socket: AsynchronousSocketChannel,
    accessors: StorageAccessor[Value]*)(
      implicit catcher: Catcher[Unit],
      m1: Manifest[Value],
      m2: Manifest[StorageAccessor[Value]]): (Array[StorageAccessor[Value]], Array[Value]) @suspendable = {
    logger.fine {
      fast"Request ${accessors.mkFastring(",")} on $socket"
    }
    def loggedCatcher: Catcher[Unit] = {
      case e: IOException =>
        logger.severe(e)
        SuspendableException.catchOrThrow(e)
    }
    val outputStream = new ByteArrayOutputStream
    val writer = new OutputStreamWriter(outputStream, "UTF-8")
    writer.write("get")
    for (accessor <- accessors) {
      val key @ KeyRegex() = accessor.key
      writer.write(' ')
      writer.write(key)
    }
    writer.write("\r\n")
    writer.flush()
    logger.finer("Request buffer is full filled.")
    logger.finest(new String(outputStream.toByteArray, "UTF-8"))
    sendAndReceive[(Array[StorageAccessor[Value]], Array[Value])](
      socket, Array(ByteBuffer.wrap(outputStream.toByteArray))) { inputStream =>
        logger.finest("GET reading...")
        val sortedAccessors = accessors.toArray
        Arrays.sort[StorageAccessor[Value]](
          sortedAccessors,
          StorageAccessorComparator)
        val values = Array.ofDim[Value](accessors.length)

        logger.finer("Waiting for response...")
        // Workaround for https://issues.scala-lang.org/browse/SI-6579
        @inline def parseLine(line: String): Unit @suspendable = {
          logger.finest { fast"GET response line: $line" }
          line match {
            case GetLineRegex(key, flags, numBytes) if numBytes.toInt <= MaxValueSize =>
              logger.fine {
                fast"Receive a header for $key(${
                  if (flags == 0)
                    fast""
                  else
                    fast"flags=$flags,"
                }numBytes=$numBytes)"
              }
              (inputStream.available = numBytes.toInt)(loggedCatcher)
              import language.postfixOps
              val index =
                Collections.binarySearch(
                  sortedAccessors.view map { _.key } asJava,
                  key)
              if (index < 0) {
                raise(
                  new ProtocolException(
                    "Response key " + key + " have not been requested."))
              } else {
                val accessor = sortedAccessors(index)
                values(index) = accessor.decode(inputStream, flags.toInt)
                if (inputStream.available > 0) {
                  logger.info {
                    fast"Decoder for $accessor does not consume all data."
                  }
                  inputStream.skip(inputStream.available)
                }

                (inputStream.available = 2)(loggedCatcher)
                if (inputStream.read() != '\r') {
                  raise(
                    new ProtocolException(
                      "There must be CRLF after binary data."))
                } else {
                  if (inputStream.read() != '\n') {
                    raise(
                      new ProtocolException(
                        "There must be CRLF after binary data."))
                  }
                }
              }
            case ServerErrorRegex(reason) =>
              raise(new ServerErrorException(reason))
            case _ =>
              raise(new ProtocolException("Bad line " + line))
          }
        }

        // Workaround for https://issues.scala-lang.org/browse/SI-6603
        var line: String = null
        line = inputStream.readUntilCRLF()
        while (line != "END\r\n") {
          parseLine(line)
          line = inputStream.readUntilCRLF()
        }
        logger.finer("Receive \"END\"")
        (sortedAccessors, values)
      }(loggedCatcher)
  }

  private val ServerErrorRegex = """SERVER_ERROR\s+(.+)\s*\r\n""".r

  final def cas[Value](
    socket: AsynchronousSocketChannel,
    accessor: StorageAccessor[Value],
    value: Value,
    casId: CasId,
    exptime: Exptime)(
      implicit catcher: Catcher[Unit]): Unit @suspendable = {
    val headOutputStream = new ByteArrayOutputStream
    val headWriter = new OutputStreamWriter(headOutputStream, "UTF-8")
    headWriter.write("cas ")
    val key @ KeyRegex() = accessor.key
    headWriter.write(key)
    headWriter.write(' ')
    val flags = accessor.getFlags(value)
    headWriter.write(flags.toString)
    headWriter.write(' ')
    headWriter.write(exptime.underlying.toString)
    headWriter.write(' ')
    val dataOutputStream = new ByteArrayOutputStream
    accessor.encode(dataOutputStream, value, flags)
    val data = dataOutputStream.toByteArray
    headWriter.write(dataOutputStream.size.toString)
    headWriter.write(' ')
    headWriter.write(casId.value.toString)
    headWriter.write("\r\n")
    headWriter.flush()
    dataOutputStream.write('\r')
    dataOutputStream.write('\n')
    dataOutputStream.flush()
    logger.fine("CAS send head: " + headOutputStream.toString("UTF-8"))
    logger.fine("CAS send data: " + dataOutputStream.toString("UTF-8"))
    sendAndReceive[Unit](socket, Array(
      ByteBuffer.wrap(headOutputStream.toByteArray),
      ByteBuffer.wrap(dataOutputStream.toByteArray))) { inputStream =>
      val line = inputStream.readUntilCRLF()
      line match {
        case ServerErrorRegex(reason) =>
          raise(new ServerErrorException(reason))
        case "NOT_STORED\r\n" =>
          raise(new NotStoredException)
        case "EXISTS\r\n" =>
          raise(new ExistsException)
        case "NOT_FOUND\r\n" =>
          raise(new NotFoundException)
        case "STORED\r\n" =>
          shiftUnit0[Unit, Unit]()
        case _ =>
          raise(new ProtocolException("Bad line " + line))
      }

    }
  }

  @throws(classOf[ExistsException])
  @throws(classOf[NotStoredException])
  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  @throws(classOf[ProtocolException])
  final def store[Value](
    socket: AsynchronousSocketChannel,
    command: String,
    accessor: StorageAccessor[Value],
    value: Value,
    exptime: Exptime)(
      implicit catcher: Catcher[Unit]): Unit @suspendable = {
    val headOutputStream = new ByteArrayOutputStream
    val headWriter = new OutputStreamWriter(headOutputStream, "UTF-8")
    headWriter.write(command)
    headWriter.write(' ')
    val key @ KeyRegex() = accessor.key
    headWriter.write(key)
    headWriter.write(' ')
    val flags = accessor.getFlags(value)
    headWriter.write(flags.toString)
    headWriter.write(' ')
    headWriter.write(exptime.underlying.toString)
    headWriter.write(' ')
    val dataOutputStream = new ByteArrayOutputStream
    accessor.encode(dataOutputStream, value, flags)
    val data = dataOutputStream.toByteArray
    headWriter.write(dataOutputStream.size.toString)
    headWriter.write("\r\n")
    headWriter.flush()
    dataOutputStream.write('\r')
    dataOutputStream.write('\n')
    dataOutputStream.flush()
    logger.fine("STORE send head: " + headOutputStream.toString("UTF-8"))
    logger.fine("STORE send data: " + dataOutputStream.toString("UTF-8"))
    sendAndReceive[Unit](
      socket,
      Array(ByteBuffer.wrap(headOutputStream.toByteArray),
        ByteBuffer.wrap(dataOutputStream.toByteArray))) { inputStream =>
        val line = inputStream.readUntilCRLF()
        line match {
          case ServerErrorRegex(reason) =>
            raise(new ServerErrorException(reason))
          case "NOT_STORED\r\n" =>
            raise(new NotStoredException)
          case "EXISTS\r\n" =>
            raise(new ExistsException)
          case "NOT_FOUND\r\n" =>
            raise(new NotFoundException)
          case "STORED\r\n" =>
            shiftUnit0[Unit, Unit]()
          case _ =>
            raise(new ProtocolException("Bad line " + line))
        }

      }
  }

  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  @throws(classOf[ProtocolException])
  final def delete[Value](
    socket: AsynchronousSocketChannel,
    accessor: StorageAccessor[Value])(
      implicit catcher: Catcher[Unit]): Unit @suspendable = {
    val headOutputStream = new ByteArrayOutputStream
    val headWriter = new OutputStreamWriter(headOutputStream, "UTF-8")
    headWriter.write("delete")
    headWriter.write(' ')
    val key @ KeyRegex() = accessor.key
    headWriter.write(key)
    headWriter.write("\r\n")
    headWriter.flush()
    logger.fine("DELETE send head: " + headOutputStream.toString("UTF-8"))
    sendAndReceive[Unit](
      socket,
      Array(ByteBuffer.wrap(headOutputStream.toByteArray))) { inputStream =>
        val line = inputStream.readUntilCRLF()
        line match {
          case ServerErrorRegex(reason) =>
            raise(new ServerErrorException(reason))
          case "NOT_FOUND\r\n" =>
            raise(new NotFoundException)
          case "DELETED\r\n" =>
            shiftUnit0[Unit, Unit]()
          case _ =>
            raise(new ProtocolException("Bad line " + line))
        }
      }
  }

  // TODO: store with "noreplay" parameter

  final def newSocket(
    group: AsynchronousChannelGroup,
    address: SocketAddress)(
      implicit catcher: Catcher[Unit]): AsynchronousSocketChannel @suspendable = {
    val socket = AsynchronousSocketChannel.open(group)
    socket.setOption(
      StandardSocketOptions.SO_KEEPALIVE,
      java.lang.Boolean.TRUE)
    socket.setOption(
      StandardSocketOptions.TCP_NODELAY,
      java.lang.Boolean.TRUE)
    shift { (continue: Void => Unit) =>
      socket.connect(
        address,
        (continue, catcher),
        ContinuationizedCompletionHandler.VoidHandler)
    }
    socket
  }
}

// vim: set sts=2 sw=2 et:
