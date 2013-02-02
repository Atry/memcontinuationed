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

object GetTest {
  implicit val (logger, formatter, appender) = ZeroLoggerFactory.newLogger(this)
}

class GetTest {
  import GetTest._

  @Test
  def got() {
    synchronized {
      var isFailed = false;
      val executor = Executors.newFixedThreadPool(1)
      val channelGroup = AsynchronousChannelGroup.withThreadPool(executor)
      val memcontinuationed = new Memcontinuationed(channelGroup, TestServerAddress.getAddress _)
      val accessor1 = new UTF8Accessor("memcontinuationed_get_test_got_1")
      val accessor2 = new UTF8Accessor("memcontinuationed_get_test_got_2")
      val accessor3 = new UTF8Accessor("memcontinuationed_get_test_got_3")

      implicit def defaultCatcher: Catcher[Unit] = {
        case e: Exception =>
          synchronized {
            isFailed = true
            notify()
          }
      }
      reset {
        memcontinuationed.add(accessor1, "1")
        memcontinuationed.add(accessor3, "3")
        val results = memcontinuationed.get(accessor1, accessor2, accessor3)
        results.get(accessor1) match {
          case None =>
            synchronized {
              isFailed = true
            }
          case Some(result) if result != "1" =>
            synchronized {
              isFailed = true
            }
          case _ =>
        }
        results.get(accessor2) match {
          case Some(result) =>
            synchronized {
              isFailed = true
            }
          case _ =>  
        }
        results.get(accessor3) match {
          case None =>
            synchronized {
              isFailed = true
            }
          case Some(result) if result != "3" =>
            synchronized {
              isFailed = true
            }
          case _ =>
        }
        synchronized {
          notify()
        }
      }
      wait()
      reset {
        memcontinuationed.delete(accessor1)
        memcontinuationed.delete(accessor3)
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
}
// vim: set ts=2 sw=2 et:


