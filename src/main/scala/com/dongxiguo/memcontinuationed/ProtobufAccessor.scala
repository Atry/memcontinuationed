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
import com.google.protobuf._

/** A key in [[https://code.google.com/p/protobuf/ Protocol Buffers]] format.
  * 
  * You should implement your own `ProtobufAccessor` for each type of value.
  *
  * @tparam Value the decoded value.
  */
trait ProtobufAccessor[Value <: Message] extends StorageAccessor[Value] {

  /** The default instance of the value */
  def defaultInstance: Value

  def encode(output: OutputStream, data: Value, flags: Int) {
    data.writeTo(output)
  }

  def decode(input: InputStream, flags: Int): Value = {
    defaultInstance.toBuilder.mergeFrom(input).build.asInstanceOf[Value]
  }
}

// vim: set ts=2 sw=2 et:
