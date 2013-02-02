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
import java.io._

/** A memcached key.
  *
  * You should implement your own `StorageAccessor` for each type of value.
  *
  * @tparam Value the decoded value.
  */
trait StorageAccessor[Value] {

  /** The underlying key sent to server */
  val key: String

  /** Returns the flags for `data`, which will be sent to server.
    *
    * @param data the value
    */
  def getFlags(data: Value): Int = 0

  /** Returns the decoded value.
    *
    * @param input the byte stream to produce the value
    * @param flags the flags for the value
    */
  def decode(input: InputStream, flags: Int): Value

  /** Encodes the value.
    *
    * @param output the byte stream the value writes to
    * @param value the value to be encoded
    * @param flags the flags for the value
    */
  def encode(output: OutputStream, value: Value, flags: Int)
}

// vim: set ts=2 sw=2 et:
