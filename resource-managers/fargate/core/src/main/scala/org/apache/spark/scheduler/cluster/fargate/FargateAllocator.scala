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

package org.apache.spark.scheduler.cluster.fargate

import scala.collection.JavaConverters._
import scala.collection.mutable

import com.amazonaws.regions.DefaultAwsRegionProviderChain
import com.amazonaws.services.ecs.AmazonECS
import com.amazonaws.services.ecs.model._

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.rpc.RpcEndpointAddress
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils

private[fargate] class FargateAllocator(conf: SparkConf, ecs: AmazonECS)
  extends Logging {

  private val DESCRIBE_TASK_MAX_TASKS = 100
  private val RUN_TASK_MAX_COUNT = 10

  private val pendingExecutors = mutable.Set[String]()

  private val runningExecutors = mutable.Set[String]()

  private val killedExecutors = mutable.Set[String]()

  private val failedExecutors = mutable.Set[String]()

  private var targetNumExecutors = 0

  private val cluster = conf.get("spark.fargate.cluster", "default")
  private val pendingMax = conf.getInt("spark.fargate.pending.max", RUN_TASK_MAX_COUNT)
  private var taskDefinition: Option[String] = None

  def initialize(appId: String) {
    val driverUrl = RpcEndpointAddress(
      conf.get("spark.driver.host"),
      conf.get("spark.driver.port").toInt,
      CoarseGrainedSchedulerBackend.ENDPOINT_NAME).toString

    val taskRole = conf.get("spark.fargate.executor.taskRole", "ecsTaskExecutionRole")
    val executionRole = conf.get("spark.fargate.executionRole", "ecsTaskExecutionRole")
    val containerImage = conf.get("spark.fargate.container.image")

    val executorCores = conf.getInt("spark.executor.cores", 1)
    val executorMemory = conf.getSizeAsMb("spark.executor.memory", "1g")
    val executorMemoryOverhead = conf.getSizeAsMb("spark.executor.memoryOverhead",
      s"${math.max(0.1 * executorMemory, 384L).toLong}m")
    val taskCpu = executorCores * 1024
    // There are a number of fix Cpu, Memory combinations that can be used for Fargate tasks.
    // Round up to the nearest Gb.
    val taskMemory = math.ceil((executorMemory + executorMemoryOverhead) / 1024d).toInt * 1024

    val userOpts = conf.getOption("spark.executor.extraJavaOptions").toSeq.
      flatMap(Utils.splitCommandString)
    val sparkOpts = Utils.sparkJavaOpts(conf, SparkConf.isExecutorStartupConf)

    val envs = {
      Seq(
        "SPARK_DRIVER_URL" -> driverUrl,
        "SPARK_EXECUTOR_CORES" -> executorCores.toString,
        "SPARK_EXECUTOR_MEMORY" -> s"${executorMemory}m",
        "SPARK_APPLICATION_ID" -> appId)
    } ++ {
      conf.getExecutorEnv
    } ++ {
      (userOpts ++ sparkOpts).zipWithIndex.map({
        case (opt, index) =>
          s"SPARK_JAVA_OPT_$index" -> opt
      })
    } ++ {
      conf.getOption("spark.executor.extraClassPath").map("SPARK_EXTRA_CLASSPATH" -> _)
    }

    val result = ecs.registerTaskDefinition(
      new RegisterTaskDefinitionRequest().
        withFamily(s"$appId-executor").
        withCpu(taskCpu.toString).
        withMemory(taskMemory.toString).
        withNetworkMode(NetworkMode.Awsvpc).
        withTaskRoleArn(taskRole).
        withExecutionRoleArn(executionRole).
        withRequiresCompatibilities(Compatibility.FARGATE).
        withContainerDefinitions(
          new ContainerDefinition().
            withName("executor").
            withCommand("executor").
            withEnvironment(
              envs.map({ case (k, v) => new KeyValuePair().withName(k).withValue(v) }).asJava).
            withImage(containerImage).
            withLogConfiguration(
              new LogConfiguration().
                withLogDriver(LogDriver.Awslogs).
                withOptions(
                  Map(
                    "awslogs-create-group" -> "true",
                    "awslogs-region" -> new DefaultAwsRegionProviderChain().getRegion,
                    "awslogs-group" -> s"/spark-fargate",
                    "awslogs-stream-prefix" -> appId
                  ).asJava
                ))))
    taskDefinition = Some(result.getTaskDefinition.getTaskDefinitionArn)
  }

  def requestTotalExecutors(requestedTotal: Int): Unit = {
    targetNumExecutors = requestedTotal
  }

  def killExecutor(executorId: String): Unit = synchronized {
    if (runningExecutors.contains(executorId)) {
      ecs.stopTask(new StopTaskRequest().withCluster(cluster).withTask(executorId))
      runningExecutors.remove(executorId)
      killedExecutors.add(executorId)
    } else if (pendingExecutors.contains(executorId)) {
      ecs.stopTask(new StopTaskRequest().withCluster(cluster).withTask(executorId))
      pendingExecutors.remove(executorId)
      killedExecutors.add(executorId)
    } else {
      logWarning(s"Attempted to kill unknown executor $executorId!")
    }
  }

  def allocateResources(): Unit = synchronized {
    updateResources()
    val additionalExecutors = math.min(RUN_TASK_MAX_COUNT,
      math.min(pendingMax - pendingExecutors.size,
        targetNumExecutors - runningExecutors.size - pendingExecutors.size))
    if (additionalExecutors > 0) {
      logInfo(s"Requesting $additionalExecutors additional executors")
      val req =
        new RunTaskRequest().
          withTaskDefinition(taskDefinition.get).
          withNetworkConfiguration(
            new NetworkConfiguration().
              withAwsvpcConfiguration(
                new AwsVpcConfiguration().
                  withAssignPublicIp(AssignPublicIp.DISABLED).
                  withSecurityGroups(conf.get("spark.fargate.executor.securityGroups").
                    split(","): _*).
                  withSubnets(conf.get("spark.fargate.executor.subnets").split(","): _*)
              )).
          withCluster(cluster).
          withLaunchType(LaunchType.FARGATE).
          withCount(additionalExecutors)
      val res = ecs.runTask(req)
      res.getTasks.asScala.foreach(task => pendingExecutors.add(task.getTaskArn.split("/").last))
      res.getFailures.asScala.foreach(failure => {
        log.warn(s"RunTaskRequest encountered failures, " +
          s"arn: ${failure.getArn}, reason: ${failure.getReason}")
      })
    }
  }

  private def updateResources(): Unit = {
    val before = Seq(
      pendingExecutors.size,
      runningExecutors.size,
      killedExecutors.size,
      failedExecutors.size
    )
    val tasks =
      (pendingExecutors ++ runningExecutors ++ killedExecutors).
        grouped(DESCRIBE_TASK_MAX_TASKS).
        map(tasks => {
          val req = new DescribeTasksRequest().withCluster(cluster).withTasks(tasks.asJava)
          val res = ecs.describeTasks(req)
          if (!res.getFailures.isEmpty) {
            val failure = res.getFailures.get(0)
            log.error(s"DescribeTasksRequest encountered ${res.getFailures.size} failures, " +
              s"arn: ${failure.getArn}, reason: ${failure.getReason}")
          }
          res.getTasks.asScala.map(task => (task.getTaskArn.split("/").last, task))
        }).toSeq.flatten.toMap
    for {
      executorId <- pendingExecutors
    } {
      val task = tasks(executorId)
      if (task.getLastStatus == "RUNNING") {
        pendingExecutors.remove(executorId)
        runningExecutors.add(executorId)
      } else if (task.getLastStatus == "STOPPED") {
        pendingExecutors.remove(executorId)
        failedExecutors.add(executorId)
      }
    }
    for {
      executorId <- runningExecutors
    } {
      val task = tasks(executorId)
      if (task.getLastStatus == "STOPPED") {
        runningExecutors.remove(executorId)
        failedExecutors.add(executorId)
      }
    }
    for {
      executorId <- killedExecutors
    } {
      val task = tasks(executorId)
      if (task.getLastStatus == "STOPPED") {
        killedExecutors.remove(executorId)
      }
    }
    val after = Seq(
      pendingExecutors.size,
      runningExecutors.size,
      killedExecutors.size,
      failedExecutors.size
    )
    if ((0 until 4).map(i => math.abs(after(i) - before(i))).sum > 0) {
      val report =
        s"Executor status report: pending: ${before(0)} -> ${after(0)}, " +
          s"running: ${before(1)} -> ${after(1)}, killed: ${before(2)} -> ${after(2)}, " +
          s"failed: ${before(3)} -> ${after(3)}"
      logInfo(report)
    }
  }

  def stop(): Unit = synchronized {
    (runningExecutors ++ pendingExecutors).foreach(task =>
      ecs.stopTask(new StopTaskRequest().withCluster(cluster).withTask(task)))
    taskDefinition.foreach(td =>
      ecs.deregisterTaskDefinition(
        new DeregisterTaskDefinitionRequest().
          withTaskDefinition(td)
      )
    )
  }

}
