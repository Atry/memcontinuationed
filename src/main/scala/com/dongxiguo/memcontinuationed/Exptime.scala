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

import java.util.Date

/** The expiration time sent to server
  *
  * @param underlying the underlying number sent to server
  */
final class Exptime private(val underlying: Long) extends AnyVal

/** Factory to create `Exptime` instance */
object Exptime {

  /** The expiration time that never be reached. */
  final val NeverExpires: Exptime = new Exptime(0L)

  /** Returns a expiration time at `date`
    *
    * @param date the expiration date
    */
  final def unixTime(date: Date): Exptime = {
    val result = (date.getTime / 1000).toInt
    if (result >= 0 && result <= 60 * 60 * 24 * 30 || result > 0xFFFFFFFFL) {
      throw new IllegalArgumentException("The date must be larger than 30 days")
    }
    new Exptime(result)
  }

  /** Returns a expiration time at some number of seconds from now on
    *
    * @param seconds the number of seconds from now on
    */
  final def fromNowOn(seconds: Int): Exptime = {
    if (seconds >= 0 && seconds <= 60 * 60 * 24 * 30) {
      new Exptime(seconds)
    } else {
      throw new IllegalArgumentException("The date must be shorter than 30 days")
    }
  }

}
