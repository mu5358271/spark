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

package org.apache.spark.deploy.fargate.submit

import scala.collection.JavaConverters.{asScalaBufferConverter, mapAsJavaMapConverter}
import scala.collection.mutable.ArrayBuffer

import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.ecs.AmazonECSClientBuilder
import com.amazonaws.services.ecs.model._
import com.amazonaws.services.logs.AWSLogsClientBuilder
import com.amazonaws.services.logs.model.{GetLogEventsRequest, ResourceNotFoundException}

import org.apache.spark.SparkConf
import org.apache.spark.deploy.SparkApplication
import org.apache.spark.internal.Logging
import org.apache.spark.util.ShutdownHookManager

private[spark] case class ClientArguments(mainAppResource: String,
                                           mainClass: String,
                                           driverArgs: Array[String])

private[spark] object ClientArguments {

  def fromCommandLineArgs(args: Array[String]): ClientArguments = {
    var mainAppResource: Option[String] = None
    var mainClass: Option[String] = None
    val driverArgs = ArrayBuffer.empty[String]

    args.sliding(2, 2).toList.foreach {
      case Array("--jar", primaryJavaResource: String) =>
        mainAppResource = Some(primaryJavaResource)
      case Array("--class", clazz: String) =>
        mainClass = Some(clazz)
      case Array("--arg", arg: String) =>
        driverArgs += arg
      case other =>
        val invalid = other.mkString(" ")
        throw new RuntimeException(s"Unknown arguments: $invalid")
    }

    require(mainClass.isDefined, "Main class must be specified via --main-class")

    ClientArguments(
      mainAppResource.get,
      mainClass.get,
      driverArgs.toArray)
  }
}

private[spark] class FargateClusterApplication extends SparkApplication with Logging {

  override def start(args: Array[String], conf: SparkConf): Unit = {
    val parsedArguments = ClientArguments.fromCommandLineArgs(args)
    run(parsedArguments, conf)
  }

  private def run(clientArguments: ClientArguments, conf: SparkConf): Unit = {
    val ecs = AmazonECSClientBuilder.defaultClient()
    val logs = AWSLogsClientBuilder.defaultClient()

    val appId = "spark-application-" + System.currentTimeMillis
    conf.set("spark.app.id", appId)

    logInfo(s"Starting application with id: $appId")

    val cluster = conf.get("spark.fargate.cluster", "default")
    val taskRole = conf.get("spark.fargate.driver.taskRole", "ecsTaskExecutionRole")
    val executionRole = conf.get("spark.fargate.executionRole", "ecsTaskExecutionRole")
    val containerImage = conf.get("spark.fargate.container.image")

    val driverCores = conf.getInt("spark.driver.cores", 1)
    val driverMemory = conf.getSizeAsMb("spark.driver.memory", "1g")
    val driverMemoryOverhead = conf.getSizeAsMb("spark.driver.memoryOverhead",
      s"${math.max(0.1 * driverMemory, 384L).toLong}m")
    val taskCpu = driverCores * 1024
    // There are a number of fix Cpu, Memory combinations that can be used for Fargate tasks.
    // Round up to the nearest Gb.
    val taskMemory = math.ceil((driverMemory + driverMemoryOverhead) / 1024d).toInt * 1024

    conf.remove("spark.jars")

    val commands = Seq("driver") ++
      conf.getOption("spark.driver.extraClassPath").toSeq.
        flatMap(Seq("--driver-class-path", _)) ++
      conf.getOption("spark.driver.extraJavaOptions").toSeq.
        flatMap(Seq("--driver-java-options", _)) ++
      conf.getOption("spark.driver.extraLibraryPath").toSeq.
        flatMap(Seq("--driver-library-path", _)) ++
      conf.getAll.flatMap({ case (k, v) => Seq("--conf", s"$k=$v")}) ++
    Seq("--class", clientArguments.mainClass) ++
    Seq(clientArguments.mainAppResource) ++
    clientArguments.driverArgs

    val taskDefinition =
      ecs.registerTaskDefinition(
      new RegisterTaskDefinitionRequest().
        withFamily(s"$appId-driver").
        withCpu(taskCpu.toString).
        withMemory(taskMemory.toString).
        withNetworkMode(NetworkMode.Awsvpc).
        withTaskRoleArn(taskRole).
        withExecutionRoleArn(executionRole).
        withRequiresCompatibilities(Compatibility.FARGATE).
        withContainerDefinitions(
          new ContainerDefinition().
            withName("driver").
            withCommand(commands: _*).
            withImage(containerImage).
            withLogConfiguration(
              new LogConfiguration().
                withLogDriver(LogDriver.Awslogs).
                withOptions(
                  Map(
                    "awslogs-create-group" -> "true",
                    "awslogs-region" -> new DefaultAwsRegionProviderChain().getRegion,
                    "awslogs-group" -> "/spark-fargate",
                    "awslogs-stream-prefix" -> appId
                  ).asJava
                )))).getTaskDefinition.getTaskDefinitionArn
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.DEFAULT_SHUTDOWN_PRIORITY - 1)(() => {
      ecs.
        deregisterTaskDefinition(
          new DeregisterTaskDefinitionRequest().
            withTaskDefinition(taskDefinition)
        )
    })
    val result =
      ecs.runTask(
      new RunTaskRequest().
      withTaskDefinition(taskDefinition).
      withNetworkConfiguration(
        new NetworkConfiguration().
          withAwsvpcConfiguration(
            new AwsVpcConfiguration().
              withAssignPublicIp(AssignPublicIp.DISABLED).
              withSecurityGroups(conf.get("spark.fargate.driver.securityGroups").split(","): _*).
              withSubnets(conf.get("spark.fargate.driver.subnets").split(","): _*)
          )).
      withCluster(cluster).
      withLaunchType(LaunchType.FARGATE).
      withCount(1)
    )
    require(result.getFailures.isEmpty,
      s"""Encountered failures:
         |${result.getFailures.asScala.map(f =>
        s"task: ${f.getArn} with reason: ${f.getReason}").mkString("\n")}
         |""".stripMargin)
    require(result.getTasks.size == 1, s"unexpected size count ${result.getTasks.size}")
    val interval = conf.getTimeAsMs("spark.fargate.report.interval", "5s")
    var task = result.getTasks.get(0)
    ShutdownHookManager.addShutdownHook(() => {
      ecs.
        stopTask(
          new StopTaskRequest().
            withTask(task.getTaskArn)
        )
    })
    while (task.getLastStatus != "STOPPED") {
      Thread.sleep(interval)
      val result =
        ecs.
          describeTasks(
            new DescribeTasksRequest().
              withCluster(cluster).
              withTasks(task.getTaskArn)
          )
      require(result.getFailures.isEmpty,
        s"""Encountered failures:
           |${result.getFailures.asScala.map(f =>
          s"task: ${f.getArn} with reason: ${f.getReason}").mkString("\n")}
           |""".stripMargin)
      require(result.getTasks.size == 1, s"unexpected size count ${result.getTasks.size}")
      task = result.getTasks.get(0)
      logInfo(s"Task ${task.getTaskArn} is ${task.getLastStatus}")
    }

    val delay = conf.getTimeAsMs("spark.fargate.logs.delay", "5s")
    Thread.sleep(delay)

    var done = false
    var nextToken: String = null
    while(!done) {
      try {
        val result =
          logs.
            getLogEvents(
              new GetLogEventsRequest().
                withLogGroupName("/spark-fargate").
                withLogStreamName(s"$appId/driver/${task.getTaskArn.split("/").last}").
                withStartFromHead(true).
                withNextToken(nextToken)
            )
        if (result.getEvents.isEmpty) {
          done = true
        } else {
          // scalastyle:off println
          result.getEvents.asScala.map(_.getMessage).foreach(System.err.println)
          // scalastyle:on println
          nextToken = result.getNextForwardToken
        }
      } catch {
        case e: ResourceNotFoundException =>
          logError("Logs not found", e)
          done = true
        case e => throw e
      }
    }
    require(task.getContainers.size == 1,
      s"unexpected container size count ${task.getContainers.size}")
    val container = task.getContainers.get(0)
    System.exit(container.getExitCode)
  }

}
