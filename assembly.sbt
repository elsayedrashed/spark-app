// Assembly
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case PathList("org", "apache", xs @ _*) => MergeStrategy.last
  case "plugin.properties" => MergeStrategy.last
  case "log4j.properties" => MergeStrategy.last
  case x => MergeStrategy.first
}

// Default Main Class
mainClass in Compile := Some("application.Main")
// File name of assembled jar
assemblyJarName in assembly := { s"${name.value}_${scalaVersion.value}-${version.value}-assembly.jar" }

