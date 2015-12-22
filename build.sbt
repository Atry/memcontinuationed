/*
 * Copyright 2013, 2014 深圳市葡萄藤网络科技有限公司 (Shenzhen Putaoteng Network Technology Co., Ltd.)
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
name := "memcontinuationed"

organization := "com.dongxiguo"

version := "0.3.3-SNAPSHOT"

libraryDependencies ++= Seq(
  "com.novocode" % "junit-interface" % "0.7" % "test->default",
  "com.typesafe" % "config" % "1.2.1" % "test"
)

libraryDependencies <++= scalaBinaryVersion {
  case "2.10" => {
    Seq()
  }
  case bv => {
    Seq("org.scala-lang.plugins" % s"scala-continuations-library_$bv" % "1.0.2")
  }
}

libraryDependencies <+= scalaVersion { sv =>
  if (sv.startsWith("2.10.")) {
    compilerPlugin("org.scala-lang.plugins" % "continuations" % sv)
  } else {
    compilerPlugin("org.scala-lang.plugins" % s"scala-continuations-plugin_$sv" % "1.0.2")
  }
}

// Make protobuf an optional library dependency
libraryDependencies += "com.google.protobuf" % "protobuf-java" % "2.4.1" % Provided

libraryDependencies += "com.dongxiguo.zero-log" %% "context" % "0.3.5"

libraryDependencies += "com.dongxiguo" %% "zero-log" % "0.3.5"

libraryDependencies += "com.dongxiguo" %% "commons-continuations" % "0.2.2"

autoCompilerPlugins := true

scalacOptions += "-deprecation"

scalacOptions += "-feature"

scalacOptions += "-unchecked"

scalacOptions += "-P:continuations:enable"

scalacOptions ++= Seq("-Xelide-below", "FINEST")

crossScalaVersions := Seq("2.10.4", "2.11.2")

publishTo <<= (isSnapshot) { isSnapshot: Boolean =>
  if (isSnapshot)
    Some("snapshots" at "https://oss.sonatype.org/content/repositories/snapshots") 
  else
    Some("releases" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
}

licenses := Seq(
  "Apache License, Version 2.0" ->
  url("http://www.apache.org/licenses/LICENSE-2.0.html"))

homepage := Some(url("https://github.com/Atry/memcontinuationed"))

pomExtra <<= scalaVersion { version =>
  <scm>
    <url>https://github.com/Atry/memcontinuationed</url>
    <connection>scm:git:git://github.com/Atry/memcontinuationed.git</connection>
  </scm>
  <developers>
    <developer>
      <id>Atry</id>
      <name>杨博</name>
    </developer>
    <developer>
      <id>yinstone</id>
      <name>杜昱宏</name>
    </developer>
  </developers>
  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <scala.version>{version}</scala.version>
  </properties>
  <build>
    <plugins>
      <plugin>
        <groupId>org.scala-tools</groupId>
        <artifactId>maven-scala-plugin</artifactId>
        <version>2.15.2</version>
        <executions>
          <execution>
            <goals>
              <goal>compile</goal>
              <goal>testCompile</goal>
            </goals>
          </execution>
        </executions>
        <configuration>
          <compilerPlugins>
            <compilerPlugin>
              <groupId>org.scala-lang.plugins</groupId>
              <artifactId>continuations</artifactId>
              <version>$&#x7b;scala.version&#x7d;</version>
            </compilerPlugin>
          </compilerPlugins>
          <recompileMode>modified-only</recompileMode>
          <args>
            <arg>-Xelide-below</arg>
            <arg>FINEST</arg>
            <arg>-deprecation</arg>
            <arg>-P:continuations:enable</arg>
          </args>
        </configuration>
      </plugin>
    </plugins>
  </build>
}

// vim: expandtab shiftwidth=2 softtabstop=2 syntax=scala
