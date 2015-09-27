/**
 * SparklineData, Inc. -- http://www.sparklinedata.com/
 *
 * Scala based Audience Behavior APIs
 *
 * Copyright 2014-2015 SparklineData, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 package org.sparkline.userAgent

import org.scalatest.FunSuite
import ua_parser.Parser;
import ua_parser.Client;


class UserAgentParsingTest extends FunSuite {

  val uaParser = new Parser();

  def parseAndPrint(uaString : String) : Unit = {
    val c: Client = uaParser.parse(uaString);
    println(uaString)
    println(s" agent_family = ${c.userAgent.family}") // => "Mobile Safari"
    println(s" agent_major  = ${c.userAgent.major}") // => "5"
    println(s" agent_minor  = ${c.userAgent.minor}") // => "1"

    println(s" os_family    = ${c.os.family}") // => "iOS"
    println(s" os_major     = ${c.os.major}") // => "5"
    println(s" os_minor     = ${c.os.minor}") // => "1"
    println(s" device_family= ${c.device.family}")

  }

  test("basic") {

    parseAndPrint("Mozilla/5.0 (iPhone; CPU iPhone OS 5_1_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B206 Safari/7534.48.3")

  }

  test("acme1") {
    parseAndPrint("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; Win64; x64; Trident/4.0; .NET CLR 2.0.50727; SLCC2; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)")

  }

  test("acme2") {
    parseAndPrint("Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; .NET4.0C; .NET4.0E)")
  }
}
