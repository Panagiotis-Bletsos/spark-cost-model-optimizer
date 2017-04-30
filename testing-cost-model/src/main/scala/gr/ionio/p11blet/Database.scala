/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gr.ionio.p11blet

import java.util.Properties

import com.typesafe.config.{Config, ConfigFactory}

object Database {
  final private val config : Config = ConfigFactory.load()
  final private val user : String = config.getString("database.user")
  final private val pass : String = config.getString("database.pass")
  final val db : String = config.getString("database.db")
  final val connectionProperties : Properties = initConnectionProperties

  def initConnectionProperties : Properties = {
    val connectionProperties = new Properties()
    connectionProperties.put("user", this.user)
    connectionProperties.put("password", this.pass)
    connectionProperties
  }
}
