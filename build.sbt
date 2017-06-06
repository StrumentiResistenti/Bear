name := "bear"

version := "0.1"

scalaVersion := "2.11.6"

libraryDependencies ++= Seq(
	// JDBC library
	"org.scalikejdbc" %% "scalikejdbc" % "3.0.0",
	
	// Logging
	"org.apache.logging.log4j" % "log4j-api" % "2.8.2",
	"org.apache.logging.log4j" % "log4j-core" % "2.8.2",
	"org.apache.logging.log4j" %% "log4j-api-scala" % "2.8.2",
	"org.slf4j" % "slf4j-nop" % "1.7.25",
	
	// Cloudera hive driver
	// "org.apache.hive" % "hive-exec" % "1.1.0-cdh5.8.5",
	// "org.apache.hive" % "hive-jdbc" % "1.1.0-cdh5.8.5",
	// "org.apache.hadoop" % "hadoop-client" % "2.6.0-mr1-cdh5.8.5",
	// "org.apache.thrift" % "libthrift" % "0.9.0",
	// "org.apache.thrift" % "libfb303" % "0.9.0",
	// "commons-logging" % "commons-logging" % "1.1.3",

	// Option parsing
	"org.backuity.clist" %% "clist-core"   % "3.2.2",
	"org.backuity.clist" %% "clist-macros" % "3.2.2" % "provided"
)

// libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.42"

// resolvers += "conjars" at "http://conjars.org/repo"

// resolvers += "clojars" at "https://clojars.org/repo"

// resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

