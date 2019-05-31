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

import java.util.concurrent.TimeUnit

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

import com.amazonaws.services.ecs.AmazonECS

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.TaskSchedulerImpl
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
import org.apache.spark.util.{ThreadUtils, Utils}


private[spark] class FargateSchedulerBackend(scheduler: TaskSchedulerImpl,
                                             sc: SparkContext,
                                             ecs: AmazonECS)
  extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {


  protected override val minRegisteredRatio =
    if (conf.getOption("spark.scheduler.minRegisteredResourcesRatio").isEmpty) {
      0.8
    } else {
      super.minRegisteredRatio
    }

  protected override def useIntegerExecutorId: Boolean = false

  private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

  private val allocatorExecutor = ThreadUtils
    .newDaemonSingleThreadScheduledExecutor("fargate-allocator")
  private val allocator = new FargateAllocator(conf, ecs)

  override def start(): Unit = {
    super.start()
    allocator.initialize(applicationId())
    allocatorExecutor.scheduleWithFixedDelay(new Runnable {
      override def run(): Unit = allocator.allocateResources()
    }, 0L, conf.getTimeAsSeconds("spark.fargate.allocate.interval", "3s"), TimeUnit.SECONDS)

    if (!Utils.isDynamicAllocationEnabled(conf)) {
      doRequestTotalExecutors(initialExecutors)
    }
  }

  override def applicationId(): String = {
    conf.getOption("spark.app.id").map(_.toString).getOrElse(super.applicationId())
  }

  override def doRequestTotalExecutors(requestedTotal: Int): Future[Boolean] = Future[Boolean] {
    allocator.requestTotalExecutors(requestedTotal)
    true
  }

  override def sufficientResourcesRegistered(): Boolean = {
    totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
  }

  override def doKillExecutors(executorIds: Seq[String]): Future[Boolean] = Future[Boolean] {
    executorIds.foreach(executorId =>
      allocator.killExecutor(executorId)
    )
    true
  }

  override def stop(): Unit = {
    Utils.tryLogNonFatalError {
      requestTotalExecutors(0, 0, Map.empty)
      allocatorExecutor.shutdown()
      allocator.stop()
    }
    super.stop()
  }

}
