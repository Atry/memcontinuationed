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

import com.dongxiguo.commons.continuations._
import com.dongxiguo.commons.continuations.CollectionConverters._
import com.dongxiguo.commons.continuations.io._
import java.nio.channels._
import java.net._
import java.util.concurrent.atomic._
import java.util.Arrays
import java.util.Collections
import scala.annotation.tailrec
import scala.util.control.Exception.Catcher
import scala.util.continuations._
import scala.collection.JavaConverters._
import com.dongxiguo.fastring.Fastring.Implicits._

private[memcontinuationed] // Workaround for https://issues.scala-lang.org/browse/SI-6585
object Memcontinuationed {
  private implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)

  private sealed abstract class Command

  private sealed abstract class OtherCommand(
    val continue: Unit => Unit)(
      implicit val catcher: Catcher[Unit]) extends Command {
    def apply(socket: AsynchronousSocketChannel)(
      implicit catcher: Catcher[Unit]): Unit @suspendable
  }

  private sealed abstract class GetOrGetsCommand extends Command

  private type GetCallback[Value] = Tuple2[Array[StorageAccessor[Value]], Array[Value]] => Unit

  private final class GetCommand[Value](
    val accessors: Array[StorageAccessor[Value]],
    val continue: GetCallback[Value])(
      implicit val catcher: Catcher[Unit])
    extends GetOrGetsCommand

  private type GetsCallback[Value] = Tuple3[Array[StorageAccessor[Value]], Array[Value], Array[CasId]] => Unit

  private final class GetsCommand[Value](
    val accessors: Array[StorageAccessor[Value]],
    val continue: GetsCallback[Value])(
      implicit val catcher: Catcher[Unit])
    extends GetOrGetsCommand

  private case object CleanUpCommand extends Command

  private final class CommandQueueBuilder(
    var getCommands: List[GetCommand[_]] = Nil,
    var getsCommands: List[GetsCommand[_]] = Nil,
    var otherCommands: List[OtherCommand] = Nil,
    var waitingCleanUp: Boolean = false)
    extends collection.mutable.Builder[Command, CommandQueue] {

    override final def ++=(that: TraversableOnce[Command]): this.type = {
      that match {
        case cq: CommandQueue =>
          getCommands = getCommands reverse_::: cq.getCommands
          getsCommands = getsCommands reverse_::: cq.getsCommands
          otherCommands = otherCommands reverse_::: cq.otherCommands
          waitingCleanUp ||= cq.isInstanceOf[WaitingCleanUpCommandQueue]
          this
        case _ =>
          super.++=(that)
      }
    }

    override final def +=(elem: Command): this.type = {
      elem match {
        case getCommand: GetCommand[_] =>
          getCommands = getCommand :: getCommands
        case getsCommand: GetsCommand[_] =>
          getsCommands = getsCommand :: getsCommands
        case otherCommand: OtherCommand =>
          otherCommands = otherCommand :: otherCommands
        case CleanUpCommand =>
          waitingCleanUp = true
      }
      this
    }

    override final def clear() {
      getCommands = Nil
      getsCommands = Nil
      otherCommands = Nil
      waitingCleanUp = false
    }
    override final def result =
      if (waitingCleanUp) {
        new CommandQueue(getCommands, getsCommands, otherCommands)
      } else {
        new WaitingCleanUpCommandQueue(getCommands, getsCommands, otherCommands)
      }
  }

  private class CommandQueue(
    val getCommands: List[GetCommand[_]],
    val getsCommands: List[GetsCommand[_]],
    val otherCommands: List[OtherCommand])
    extends Traversable[Command] with collection.TraversableLike[Command, CommandQueue] {
    override def foreach[U](f: Command => U) {
      otherCommands.foreach(f)
      getCommands.foreach(f)
      getsCommands.foreach(f)
    }

    override protected final def newBuilder = new CommandQueueBuilder

    override final def seq = this
  }

  private final class WaitingCleanUpCommandQueue(
    getCommands: List[GetCommand[_]],
    getsCommands: List[GetsCommand[_]],
    otherCommands: List[OtherCommand])
    extends CommandQueue(getCommands, getsCommands, otherCommands) {
    override final def foreach[U](f: Command => U) {
      super.foreach(f)
      f(CleanUpCommand)
    }
  }

}

final class Memcontinuationed(
  group: AsynchronousChannelGroup,
  locator: StorageAccessor[_] => SocketAddress) {
  import Memcontinuationed._
  
  logger.fine("Creating Memcontinuationed...")

  private final class CommandRunner(address: SocketAddress)
    extends SequentialRunner[Command, CommandQueue] {

    @volatile private var _socket: AsynchronousSocketChannel = null

    // 获取socket，如果未连接，则自动重连
    def socket(implicit catcher: Catcher[Unit]): AsynchronousSocketChannel @suspendable = {
      if (_socket == null || !_socket.isOpen) {
        _socket = AsciiProtocol.newSocket(group, address)
        _socket
      } else {
        _socket
      }
    }

    @inline private def forceContinue[A, B, C](
      command: GetsCommand[A],
      accessors: Array[StorageAccessor[B]],
      values: Array[C],
      casIDs: Array[CasId]) {
      command.continue(
        accessors.asInstanceOf[Array[StorageAccessor[A]]],
        values.asInstanceOf[Array[A]],
        casIDs)
    }

    @inline private def forceContinue[A, B, C](
      command: GetCommand[A],
      accessors: Array[StorageAccessor[B]],
      values: Array[C]) {
      command.continue(
        accessors.asInstanceOf[Array[StorageAccessor[A]]],
        values.asInstanceOf[Array[A]])
    }

    private def runGet(tasks: CommandQueue)(implicit catcher: Catcher[Unit]) = {
      val getAccessorIterator =
        tasks.getCommands.iterator.flatMap[StorageAccessor[_]] {
          _.accessors
        }
      val accessors = getAccessorIterator.toArray

      // Workaround for https://issues.scala-lang.org/browse/SI-6588
      AsciiProtocol.get(socket, accessors: _*) match {
        case (responseAccessors, values) => {
          for (getCommand <- tasks.getCommands) {
            forceContinue(getCommand, responseAccessors, values)
          }
        }
      }
    }

    private def runGets(
      tasks: CommandQueue)(
        implicit catcher: Catcher[Unit]) = {
      val getAccessorIterator =
        tasks.getCommands.iterator.flatMap[StorageAccessor[_]] {
          _.accessors
        }
      val getsAccessorIterator =
        tasks.getsCommands.iterator.flatMap[StorageAccessor[_]] {
          _.accessors
        }
      val accessors = (getAccessorIterator ++ getsAccessorIterator).toArray

      AsciiProtocol.gets(socket, accessors: _*) match {
        case (responseAccessors, values, casIDs) => {
          for (getCommand <- tasks.getCommands) {
            forceContinue(getCommand, responseAccessors, values)
          }
          for (getsCommand <- tasks.getsCommands) {
            forceContinue(getsCommand, responseAccessors, values, casIDs)
          }
        }
      }
    }

    private def runOther(
      command: OtherCommand)(
        implicit catcher: Catcher[Unit]): Unit @suspendable = {
      logger.finest("Before OtherCommand.apply()")
      command(socket)
      logger.finest("After OtherCommand.apply()")
      command.continue()
    }

    override protected final def consumeSome(tasks: CommandQueue): CommandQueue @suspendable = {
      tasks.otherCommands match {
        case Nil => {
          if (tasks.isInstanceOf[WaitingCleanUpCommandQueue] &&
            tasks.getCommands.isEmpty && tasks.getsCommands.isEmpty) {
            if (_socket != null) {
              _socket.close()
              _socket = null
            }
            new CommandQueue(Nil, Nil, Nil)
          } else {
            shift { (continue: CommandQueue => Unit) =>
              implicit val catcher: Catcher[Unit] = {
                case e: Exception =>
                  for (getsCommand <- tasks.getsCommands) {
                    if (getsCommand.catcher.isDefinedAt(e)) {
                      getsCommand.catcher(e)
                    }
                  }
                  for (getCommand <- tasks.getCommands) {
                    if (getCommand.catcher.isDefinedAt(e)) {
                      getCommand.catcher(e)
                    }
                  }
                  // 如果不是逻辑错误，则杀掉连接
                  if (!e.isInstanceOf[LogicException] && _socket != null) {
                    _socket.close()
                    _socket = null
                  }
                  continue(new CommandQueue(Nil, Nil, tasks.otherCommands))
              }
              reset {
                if (tasks.getsCommands.nonEmpty) {
                  runGets(tasks)
                } else {
                  runGet(tasks)
                }
                continue(new CommandQueue(Nil, Nil, tasks.otherCommands))
              }
            }
          }
        }
        case otherCommands => {
          logger.fine {
            fast"${otherCommands.size} OtherCommands remainning."
          }
          def command = otherCommands.head
          shift { (continue: CommandQueue => Unit) =>
            implicit val catcher: Catcher[Unit] = {
              case e: Exception =>
                if (command.catcher.isDefinedAt(e)) {
                  command.catcher(e)
                }
                // 如果不是逻辑错误，则杀掉连接
                if (!e.isInstanceOf[LogicException] && _socket != null) {
                  _socket.close()
                  _socket = null
                }
                continue(
                  new CommandQueue(
                    tasks.getCommands,
                    tasks.getsCommands,
                    otherCommands.tail))
            }
            reset[Unit, Unit] {
              runOther(command)
              continue(
                new CommandQueue(
                  tasks.getCommands,
                  tasks.getsCommands,
                  otherCommands.tail))
            }
          }
        }
      }
    }

    override protected final def taskQueueCanBuildFrom =
      new collection.generic.CanBuildFrom[CommandQueue, Command, CommandQueue] {
        def apply() = new CommandQueueBuilder
        def apply(from: CommandQueue) = new CommandQueueBuilder()
      }
  }

  private val runnerMapReference =
    new AtomicReference(Map.empty[SocketAddress, CommandRunner])

  private final class ShuttedDown extends Map[SocketAddress, CommandRunner] {
    override final def +[B1 >: CommandRunner](kv: (SocketAddress, B1)) =
      throw new IllegalStateException("Memcontinuationed is shutted down.")

    override final def -(key: SocketAddress) =
      throw new IllegalStateException("Memcontinuationed is shutted down.")

    override final def get(key: SocketAddress) =
      throw new IllegalStateException("Memcontinuationed is shutted down.")

    override final def iterator =
      throw new IllegalStateException("Memcontinuationed is shutted down.")

  }

  /* @tailrec */ // FIXME: Workaround for https://issues.scala-lang.org/browse/SI-6589
  private def getCommandRunner(
    address: SocketAddress)(
      implicit catcher: Catcher[Unit]): CommandRunner @suspendable = {
    val runnerMap = runnerMapReference.get
    if (runnerMap.isInstanceOf[ShuttedDown]) {
      val e = new ShuttedDownException("Memcontinuationed is shutted down.")
      if (catcher.isDefinedAt(e)) {
        catcher(e)
      } else {
        throw e
      }
      shift(Hang)
    } else {
      runnerMap.get(address) match {
        case None =>
          val runner = new CommandRunner(address)
          val newRunnerMap: Map[SocketAddress, CommandRunner] = runnerMap.updated(address, runner)
          if (runnerMapReference.compareAndSet(runnerMap, newRunnerMap)) {
            runner
          } else {
            // retry
            getCommandRunner(address)
          }
        case Some(runner) =>
          runner
      }
    }
  }

  private def getCommandRunnerForAccessor(
    accessor: StorageAccessor[_])(
      implicit catcher: Catcher[Unit]) =
    getCommandRunner(locator(accessor))

  final def shutdown() {
    runnerMapReference.getAndSet(new ShuttedDown).values foreach {
      _.shutDown(CleanUpCommand)
    }
  }

  @throws(classOf[ProtocolException])
  @throws(classOf[ExistsException])
  @throws(classOf[NotStoredException])
  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  private def store[Value](command: String)(
    accessor: StorageAccessor[Value],
    value: Value,
    exptime: Exptime = Exptime.NeverExpires)(
      implicit catcher: Catcher[Unit]): Unit @suspendable = {
    val commandRunner = getCommandRunnerForAccessor(accessor)
    shift { (continue: Unit => Unit) =>
      logger.finer("Enqueue store command.")
      try {
        commandRunner.enqueue(new OtherCommand(continue) {
          override final def apply(
            socket: AsynchronousSocketChannel)(
              implicit catcher: Catcher[Unit]): Unit @suspendable = {
            AsciiProtocol.store(socket, command, accessor, value, exptime)
          }
        })
        commandRunner.flush()
      } catch {
        case e if catcher.isDefinedAt(e) =>
          catcher(e)
      }
    }
  }

  @throws(classOf[ProtocolException])
  @throws(classOf[ExistsException])
  @throws(classOf[NotStoredException])
  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  final def set[Value](
    accessor: StorageAccessor[Value],
    value: Value,
    exptime: Exptime = Exptime.NeverExpires)(
      implicit catcher: Catcher[Unit]): Unit @suspendable =
    store("set")(accessor, value, exptime)

  @throws(classOf[ProtocolException])
  @throws(classOf[ExistsException])
  @throws(classOf[NotStoredException])
  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  final def add[Value](
    accessor: StorageAccessor[Value],
    value: Value,
    exptime: Exptime = Exptime.NeverExpires)(
      implicit catcher: Catcher[Unit]): Unit @suspendable =
    store("add")(accessor, value, exptime)

  @throws(classOf[ProtocolException])
  @throws(classOf[ExistsException])
  @throws(classOf[NotStoredException])
  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  final def replace[Value](
    accessor: StorageAccessor[Value],
    value: Value,
    exptime: Exptime = Exptime.NeverExpires)(
      implicit catcher: Catcher[Unit]): Unit @suspendable =
    store("replace")(accessor, value, exptime)

  @throws(classOf[ProtocolException])
  @throws(classOf[ExistsException])
  @throws(classOf[NotStoredException])
  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  final def append[Value](
    accessor: StorageAccessor[Value],
    value: Value,
    exptime: Exptime = Exptime.NeverExpires)(
      implicit catcher: Catcher[Unit]): Unit @suspendable =
    store("append")(accessor, value, exptime)

  @throws(classOf[ProtocolException])
  @throws(classOf[ExistsException])
  @throws(classOf[NotStoredException])
  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  final def prepend[Value](
    accessor: StorageAccessor[Value],
    value: Value,
    exptime: Exptime = Exptime.NeverExpires)(
      implicit catcher: Catcher[Unit]): Unit @suspendable =
    store("prepend")(accessor, value, exptime)

  final class Mutable[Value] private[Memcontinuationed] (
    val accessor: StorageAccessor[Value],
    val origin: Value,
    casID: CasId) {

    override final def toString =
      "Mutable(key=" + accessor.key +
        ",value=" + origin +
        "casID=" + casID + ")"

    @throws(classOf[ProtocolException])
    @throws(classOf[ExistsException])
    @throws(classOf[NotStoredException])
    @throws(classOf[NotFoundException])
    @throws(classOf[ServerErrorException])
    final def cas(newValue: Value, exptime: Exptime = Exptime.NeverExpires)(
      implicit catcher: Catcher[Unit]): Unit @suspendable = {
      val commandRunner = getCommandRunnerForAccessor(accessor)
      shift { (continue: Unit => Unit) =>
        logger.finer("Enqueue CAS command.")
        try {
          commandRunner.enqueue(new OtherCommand(continue) {
            override final def apply(
              socket: AsynchronousSocketChannel)(
                implicit catcher: Catcher[Unit]): Unit @suspendable = {
              AsciiProtocol.cas(socket, accessor, newValue, casID, exptime)
            }
          })
          commandRunner.flush()
        } catch {
          case e if catcher.isDefinedAt(e) =>
            catcher(e)
        }
      }
    }

    /**
     * @param mutator 如果mutator的返回值和origin相等，
     * 则不用存数据而直接成功
     */
    @throws(classOf[ProtocolException])
    @throws(classOf[NotStoredException])
    @throws(classOf[NotFoundException])
    @throws(classOf[ServerErrorException])
    private def asyncContinuationizedCASUntilSuccess(
      mutator: Value => Value @suspendable,
      exptime: Exptime = Exptime.NeverExpires)(
        continue: Unit => Unit)(
          implicit catcher: Catcher[Unit],
          m: Manifest[Value]) {
      reset {
        val newValue = mutator(origin)
        if (newValue != origin) {
          cas(newValue, exptime) {
            case e: ExistsException =>
              reset {
                val newMutableMap = gets(accessor)
                newMutableMap.get(accessor) match {
                  case None =>
                    val notFoundException = new NotFoundException
                    if (!catcher.isDefinedAt(notFoundException)) {
                      throw notFoundException
                    }
                    catcher(notFoundException)
                  case Some(newMutable) =>
                    newMutable.asyncContinuationizedCASUntilSuccess(
                      mutator,
                      exptime)(continue)
                }
              }
            case e if catcher.isDefinedAt(e) =>
              catcher(e)
          }
        } else shiftUnit0[Unit, Unit]()
        continue()
      }
    }

    @throws(classOf[ProtocolException])
    @throws(classOf[NotStoredException])
    @throws(classOf[NotFoundException])
    @throws(classOf[ServerErrorException])
    final def continuationizedCASUntilSuccess(
      mutator: Value => Value @suspendable,
      exptime: Exptime = Exptime.NeverExpires)(
        implicit catcher: Catcher[Unit],
        m: Manifest[Value]): Unit @suspendable = {
      shift { (continue: Unit => Unit) =>
        asyncContinuationizedCASUntilSuccess(mutator, exptime)(continue)
      }
    }

    /**
     * @param mutator 如果mutator的返回值和origin相等，
     * 则不用存数据而直接成功
     */
    @throws(classOf[ProtocolException])
    @throws(classOf[NotStoredException])
    @throws(classOf[NotFoundException])
    @throws(classOf[ServerErrorException])
    private def asyncCASUntilSuccess(
      mutator: Value => Value,
      exptime: Exptime = Exptime.NeverExpires)(
        continue: Unit => Unit)(
          implicit catcher: Catcher[Unit],
          m: Manifest[Value]) {
      reset {
        val newValue = mutator(origin)
        if (newValue != origin) {
          cas(newValue, exptime) {
            case e: ExistsException =>
              reset {
                val newMutableMap = gets(accessor)
                newMutableMap.get(accessor) match {
                  case None =>
                    val notFoundException = new NotFoundException
                    if (!catcher.isDefinedAt(notFoundException)) {
                      throw notFoundException
                    }
                    catcher(notFoundException)
                  case Some(newMutable) =>
                    newMutable.asyncCASUntilSuccess(
                      mutator,
                      exptime)(continue)
                }
              }
            case e if catcher.isDefinedAt(e) =>
              catcher(e)
          }
        } else shiftUnit0[Unit, Unit]()
        continue()
      }
    }

    @throws(classOf[ProtocolException])
    @throws(classOf[NotStoredException])
    @throws(classOf[NotFoundException])
    @throws(classOf[ServerErrorException])
    final def casUntilSuccess(
      mutator: Value => Value,
      exptime: Exptime = Exptime.NeverExpires)(
        implicit catcher: Catcher[Unit],
        m: Manifest[Value]): Unit @suspendable = {
      shift { (continue: Unit => Unit) =>
        asyncCASUntilSuccess(mutator, exptime)(continue)
      }
    }
  }

  @throws(classOf[ProtocolException])
  @throws(classOf[ServerErrorException])
  final def addOrAppend[Value](
    accessor: StorageAccessor[Value],
    value: Value,
    exptime: Exptime = Exptime.NeverExpires)(
      implicit catcher: Catcher[Unit]): Unit @suspendable = {
    shift { (continue: Unit => Unit) =>
      reset {
        add(accessor, value, exptime) {
          case e: NotStoredException =>
            reset {
              append(accessor, value, exptime)
              continue()
            }
          case e if catcher.isDefinedAt(e) =>
            catcher(e)
        }
        continue()
      }
    }
  }

  private[Memcontinuationed] final class GetsResult[Value](
    allRequestedAccessors: Array[StorageAccessor[Value]],
    resultsByAddress: Map[SocketAddress, (Array[StorageAccessor[Value]], Array[Value], Array[CasId])])
    extends Map[StorageAccessor[Value], Mutable[Value]] {
    logger.finest {
      fast"Create new GetsResult $this"
    }

    override final def iterator = {
      import language.postfixOps
      for {
        accessor <- allRequestedAccessors.iterator
        address = locator(accessor)
        (accessors, values, casIDs) = resultsByAddress(address)
        i = Collections.binarySearch(
          accessors.view.map { _.key } asJava,
          accessor.key)
        if i >= 0
        value = values(i)
        if value != null
      } yield accessor -> new Mutable[Value](accessor, value, casIDs(i))
    }

    override final def +[B1 >: Mutable[Value]](
      kv: (StorageAccessor[Value], B1)): Map[StorageAccessor[Value], B1] =
      (iterator ++ Seq(kv)).toMap

    override final def -(
      accessor: StorageAccessor[Value]): Map[StorageAccessor[Value], Mutable[Value]] = {
      import language.postfixOps
      iterator filter { _._1.key != accessor.key } toMap
    }

    override final def get(
      accessor: StorageAccessor[Value]): Option[Mutable[Value]] = {
      val address = locator(accessor)
      val (accessors, values, casIDs) = resultsByAddress(address)
      import language.postfixOps
      val i = Collections.binarySearch(
        accessors.view map { _.key } asJava,
        accessor.key)
      if (i < 0 ||
        Arrays.binarySearch[StorageAccessor[Value]](
          allRequestedAccessors,
          accessor,
          StorageAccessorComparator) < 0) {
        None
      } else {
        assert(accessors(i) == accessor)
        values(i) match {
          case null =>
            None
          case value =>
            Some(new Mutable(accessors(i), value, casIDs(i)))
        }
      }
    }
  }

  /**
   * @return 服务器中找到的键值对，可能少于`allAccessors`请求的键。
   */
  @throws(classOf[ProtocolException])
  @throws(classOf[ServerErrorException])
  final def gets[Value](allAccessors: StorageAccessor[Value]*)(
    implicit catcher: Catcher[Unit], m: Manifest[Value]): Map[StorageAccessor[Value], Mutable[Value]] @suspendable = {
    val groupedAccessors = allAccessors.groupBy(locator)
    val results = groupedAccessors.asSuspendable.par map { entry =>
      val (address, accessors) = entry
      val commandRunner = getCommandRunner(address)
      val result = shift { continue: GetsCallback[Value] =>
        try {
          commandRunner.enqueue(new GetsCommand[Value](accessors.toArray, continue))
          commandRunner.flush()
        } catch {
          case e if catcher.isDefinedAt(e) =>
            catcher(e)
        }
      }
      address -> result
    }
    val sortedAccessors = allAccessors.toArray
    Arrays.sort[StorageAccessor[Value]](
      sortedAccessors,
      StorageAccessorComparator)
    new GetsResult(sortedAccessors, results.toMap)
  }

  /**
   * 获取一个键值对以用于修改
   * @note 如果需要检查数据是否存在，请使用[[com.dongxiguo.memcontinuationed.Memcontinuationed#gets]]
   */
  @throws(classOf[ProtocolException])
  @throws(classOf[ServerErrorException])
  @throws(classOf[NotFoundException])
  final def requireForCAS[Value](accessor: StorageAccessor[Value])(
    implicit catcher: Catcher[Unit], m: Manifest[Value]): Mutable[Value] @suspendable = {
    val address = locator(accessor)
    val commandRunner = getCommandRunner(address)
    shift { (continue: Mutable[Value] => Unit) =>
      try {
        commandRunner.enqueue(
          new GetsCommand[Value](
            Array(accessor),
            { result: (Array[StorageAccessor[Value]], Array[Value], Array[CasId]) =>
              val (accessors, values, casIDs) = result
              import language.postfixOps
              val i = Collections.binarySearch(
                accessors.view map { _.key } asJava,
                accessor.key)
              if (i < 0) {
                val e = new NotFoundException
                if (catcher.isDefinedAt(e)) {
                  catcher(e)
                } else {
                  throw e
                }
              } else {
                values(i) match {
                  case null =>
                    val e = new NotFoundException
                    if (catcher.isDefinedAt(e)) {
                      catcher(e)
                    } else {
                      throw e
                    }
                  case value =>
                    continue(new Mutable[Value](accessor, value, casIDs(i)))
                }
              }
            }))
        commandRunner.flush()
      } catch {
        case e if catcher.isDefinedAt(e) =>
          catcher(e)
      }
    }
  }

  private[Memcontinuationed] final class GetResult[Value](
    allRequestedAccessors: Array[StorageAccessor[Value]],
    resultsByAddress: Map[SocketAddress, (Array[StorageAccessor[Value]], Array[Value])])
    extends Map[StorageAccessor[Value], Value] {
    override final def iterator = {
      import language.postfixOps
      for {
        accessor <- allRequestedAccessors.iterator
        address = locator(accessor)
        (accessors, values) = resultsByAddress(address)
        i = Collections.binarySearch(
          accessors.view.map { _.key } asJava,
          accessor.key)
        if i >= 0
        value = values(i)
        if value != null
      } yield accessor -> value
    }

    override final def +[B1 >: Value](
      kv: (StorageAccessor[Value], B1)): Map[StorageAccessor[Value], B1] =
      (iterator ++ Seq(kv)).toMap

    override final def -(
      accessor: StorageAccessor[Value]): Map[StorageAccessor[Value], Value] = {
      import language.postfixOps
      iterator filter { _._1.key != accessor.key } toMap
    }

    override final def get(
      accessor: StorageAccessor[Value]): Option[Value] = {
      val address = locator(accessor)
      val (accessors, values) = resultsByAddress(address)
      import language.postfixOps
      val i = Collections.binarySearch(
        accessors.view map { _.key } asJava,
        accessor.key)
      if (i < 0 ||
        Arrays.binarySearch[StorageAccessor[Value]](
          allRequestedAccessors,
          accessor,
          StorageAccessorComparator) < 0) {
        None
      } else {
        assert(accessors(i) == accessor)
        Option(values(i))
      }
    }
  }

  /**
   * @return 服务器中找到的键值对，可能少于`allAccessors`请求的键。
   */
  @throws(classOf[ProtocolException])
  @throws(classOf[ServerErrorException])
  final def get[Value](allAccessors: StorageAccessor[Value]*)(
    implicit catcher: Catcher[Unit], m: Manifest[Value]): Map[StorageAccessor[Value], Value] @suspendable = {
    logger.finer("get")
    val groupedAccessors = allAccessors.groupBy(locator)
    val results = groupedAccessors.asSuspendable.par map { entry =>
      val (address, accessors) = entry
      logger.finer("getCommandRunner")
      val commandRunner = getCommandRunner(address)
      val result = shift { continue: GetCallback[Value] =>
        logger.finer("enqueue")
        try {
          commandRunner.enqueue(new GetCommand[Value](accessors.toArray, continue))
          commandRunner.flush()
        } catch {
          case e if catcher.isDefinedAt(e) =>
            catcher(e)
        }
      }
      address -> result
    }
    val sortedAccessors = allAccessors.toArray
    Arrays.sort[StorageAccessor[Value]](
      sortedAccessors,
      StorageAccessorComparator)
    new GetResult(sortedAccessors, results.toMap)
  }

  /**
   * 获取一个键值对
   * @note 如果需要检查数据是否存在，请使用[[com.dongxiguo.memcontinuationed.Memcontinuationed#gets]]
   */
  @throws(classOf[ProtocolException])
  @throws(classOf[ServerErrorException])
  @throws(classOf[NotFoundException])
  final def require[Value](accessor: StorageAccessor[Value])(
    implicit catcher: Catcher[Unit], m: Manifest[Value]): Value @suspendable = {
    logger.fine("REQUIRE " + accessor)
    val address = locator(accessor)
    val commandRunner = getCommandRunner(address)
    shift { (continue: Value => Unit) =>
      try {
        commandRunner.enqueue(
          new GetCommand[Value](
            Array(accessor),
            { result: (Array[StorageAccessor[Value]], Array[Value]) =>
              val (accessors, values) = result
              import language.postfixOps
              val i = Collections.binarySearch(
                accessors.view map { _.key } asJava,
                accessor.key)
              if (i < 0) {
                val e = new NotFoundException
                if (catcher.isDefinedAt(e)) {
                  catcher(e)
                } else {
                  throw e
                }
              } else {
                values(i) match {
                  case null =>
                    val e = new NotFoundException
                    if (catcher.isDefinedAt(e)) {
                      catcher(e)
                    } else {
                      throw e
                    }
                  case value =>
                    continue(value)
                }
              }
            }))
        commandRunner.flush()
      } catch {
        case e if catcher.isDefinedAt(e) =>
          catcher(e)
      }
    }
  }

  @throws(classOf[NotFoundException])
  @throws(classOf[ServerErrorException])
  @throws(classOf[ProtocolException])
  final def delete[Value](accessor: StorageAccessor[Value])(
    implicit catcher: Catcher[Unit]): Unit @suspendable = {
    val commandRunner = getCommandRunnerForAccessor(accessor)
    shift { (continue: Unit => Unit) =>
      logger.finer("Enqueue store command.")
      try {
        commandRunner.enqueue(new OtherCommand(continue) {
          override final def apply(
            socket: AsynchronousSocketChannel)(
              implicit catcher: Catcher[Unit]): Unit @suspendable = {
            AsciiProtocol.delete(socket, accessor)
          }
        })
        commandRunner.flush()
      } catch {
        case e if catcher.isDefinedAt(e) =>
          catcher(e)
      }
    }
  }
}

// vim: set ts=2 sw=2 et:
