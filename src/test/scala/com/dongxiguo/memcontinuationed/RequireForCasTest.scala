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

package com
package dongxiguo
package memcontinuationed

import org.junit._
import java.util.concurrent._
import java.nio.channels._
import java.io._
import scala.util.continuations._
import scala.util.control.Exception.Catcher
import com.dongxiguo.commons.continuations.CollectionConverters._

object RequireForCasTest {
  implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)
}

class RequireForCasTest {
  import RequireForCasTest._
  
  @Test
  def cas() {
    synchronized {
      var isFailed = false;
      val executor = Executors.newFixedThreadPool(1)
      val channelGroup = AsynchronousChannelGroup.withThreadPool(executor)
      val memcontinuationed = new Memcontinuationed(channelGroup, TestServerAddress.getAddress _)
      val accessor = new UTF8Accessor("memcontinuationed_requireForCas_test_cas")

      implicit def defaultCatcher: Catcher[Unit] = {
        case e: Exception =>
          synchronized {
            isFailed = true
            notify()
          }
      }
      reset {
        memcontinuationed.add(accessor, "1")
        val mutable = memcontinuationed.requireForCAS(accessor)
	      mutable.casUntilSuccess{ origin =>
	        origin + "2"
	      }
        val result = memcontinuationed.require(accessor)
        synchronized {
          result match {
            case "12" =>
            case _ =>
              isFailed = true
          }
          notify()
        }
      }
      wait()
      reset {
        memcontinuationed.delete(accessor)
        synchronized {
          notify()
        }
      }
      wait()
      memcontinuationed.shutdown()
      channelGroup.shutdown()
      if (isFailed) {
        Assert.fail()
      }
    }
  }

  @Test
  def continuationedCas() {
    synchronized {
      var isFailed = false;
      val executor = Executors.newFixedThreadPool(1)
      val channelGroup = AsynchronousChannelGroup.withThreadPool(executor)
      val memcontinuationed = new Memcontinuationed(channelGroup, TestServerAddress.getAddress _)
      val accessor = new UTF8Accessor("memcontinuationed_requireForCas_test_continuationedCas")

      implicit def defaultCatcher: Catcher[Unit] = {
        case e: Exception =>
          synchronized {
            isFailed = true
            notify()
          }
      }
      reset {
        memcontinuationed.add(accessor, "1")
        val mutable = memcontinuationed.requireForCAS(accessor)
	      mutable.continuationizedCASUntilSuccess { origin =>
	        shift { (countinue: String => Unit) =>
            synchronized {
              notify()
            }
          }
          synchronized {
            isFailed = true
            notify()
          }
          origin
	      }
        synchronized {
          isFailed = true
          notify()
        }
      }
      wait()
      reset {
        memcontinuationed.delete(accessor)
        synchronized {
          notify()
        }
      }
      wait()
      memcontinuationed.shutdown()
      channelGroup.shutdown()
      if (isFailed) {
        Assert.fail()
      }
    }
  }


  @Test
  def notFound() {
    synchronized {
      var isFailed = false;
      val executor = Executors.newFixedThreadPool(1)
      val channelGroup = AsynchronousChannelGroup.withThreadPool(executor)
      val memcontinuationed = new Memcontinuationed(channelGroup, TestServerAddress.getAddress _)
      val accessor = new UTF8Accessor("memcontinuationed_requireForCas_test_notFound")

      implicit def defaultCatcher: Catcher[Unit] = {
        case e: Exception =>
          synchronized {
            isFailed = true
            notify()
          }
      }
      reset {
        memcontinuationed.requireForCAS(accessor)({
          case e: NotFoundException =>
            synchronized {
              notify()
            }
          case e: Exception =>
            synchronized {
              isFailed = true
              notify()
           }
        }, manifest[String])
        synchronized {
          isFailed = true
          notify()
        }
      }
      wait()
      memcontinuationed.shutdown()
      channelGroup.shutdown()
      if (isFailed) {
        Assert.fail()
      }
    }
  }
}

// vim: set ts=2 sw=2 et:


