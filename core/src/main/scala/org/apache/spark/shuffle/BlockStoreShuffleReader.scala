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

package org.apache.spark.shuffle

import java.io.DataInputStream

import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.GetObjectRequest
import org.apache.hadoop.fs.Path

import org.apache.spark._
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.CompletionIterator
import org.apache.spark.util.collection.ExternalSorter

/**
 * Fetches and reads the partitions in range [startPartition, endPartition) from a shuffle by
 * requesting them from other nodes' block stores.
 */
private[spark] class BlockStoreShuffleReader[K, C](
    handle: BaseShuffleHandle[K, _, C],
    startPartition: Int,
    endPartition: Int,
    context: TaskContext,
    readMetrics: ShuffleReadMetricsReporter,
    serializerManager: SerializerManager = SparkEnv.get.serializerManager,
    blockManager: BlockManager = SparkEnv.get.blockManager,
    mapOutputTracker: MapOutputTracker = SparkEnv.get.mapOutputTracker)
  extends ShuffleReader[K, C] with Logging {

  private val dep = handle.dependency

  /** Read the combined key-values for this reduce task */
  override def read(): Iterator[Product2[K, C]] = {
    val conf = SparkEnv.get.conf
    val wrappedStreams =
      if (conf.getBoolean("spark.shuffle.s3.enabled", false)) {
        logInfo("reading shuffle files from S3")
        val s3 = AmazonS3ClientBuilder.defaultClient()
        val bucket = conf.get("spark.shuffle.s3.bucket")
        val parent = conf.get("spark.shuffle.s3.prefix", ".sparkStaging") +
          Path.SEPARATOR + conf.get("spark.app.id") + Path.SEPARATOR + "shuffle"
        mapOutputTracker.
          getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition).
          flatMap(_._2).
          map({
            case (blockId@ShuffleBlockId(shuffleId, mapId, reduceId), size) =>
              val indexKey = parent + Path.SEPARATOR +
                ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
              val index = new DataInputStream(s3.getObject(new GetObjectRequest(bucket, indexKey).
                withRange(reduceId * 8L, reduceId * 8L + 16 - 1)).getObjectContent)
              val offset = index.readLong()
              val nextOffset = index.readLong()
              index.close()
              val dataKey = parent + Path.SEPARATOR +
                ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID).name
              val data = s3.getObject(new GetObjectRequest(bucket, dataKey).
                withRange(offset, nextOffset - 1)).getObjectContent
              logInfo(s"Fetched $blockId, $offset to $nextOffset " +
                s"from $dataKey based on $indexKey with estimated size $size")
              (blockId, serializerManager.wrapStream(blockId, data))
            case (blockId, _) => throw new IllegalStateException(s"$blockId is not a shuffle block")
          })
      } else {
        new ShuffleBlockFetcherIterator(
          context,
          blockManager.shuffleClient,
          blockManager,
          mapOutputTracker.getMapSizesByExecutorId(handle.shuffleId, startPartition, endPartition),
          serializerManager.wrapStream,
          // Note: we use getSizeAsMb when no suffix is provided for backwards compatibility
          conf.get(config.REDUCER_MAX_SIZE_IN_FLIGHT) * 1024 * 1024,
          conf.get(config.REDUCER_MAX_REQS_IN_FLIGHT),
          conf.get(config.REDUCER_MAX_BLOCKS_IN_FLIGHT_PER_ADDRESS),
          conf.get(config.MAX_REMOTE_BLOCK_SIZE_FETCH_TO_MEM),
          conf.get(config.SHUFFLE_DETECT_CORRUPT),
          conf.get(config.SHUFFLE_DETECT_CORRUPT_MEMORY),
          readMetrics).toCompletionIterator
      }
    val serializerInstance = dep.serializer.newInstance()

    // Create a key/value iterator for each stream
    val recordIter = wrappedStreams.flatMap { case (blockId, wrappedStream) =>
      // Note: the asKeyValueIterator below wraps a key/value iterator inside of a
      // NextIterator. The NextIterator makes sure that close() is called on the
      // underlying InputStream when all records have been read.
      serializerInstance.deserializeStream(wrappedStream).asKeyValueIterator
    }

    // Update the context task metrics for each record read.
    val metricIter = CompletionIterator[(Any, Any), Iterator[(Any, Any)]](
      recordIter.map { record =>
        readMetrics.incRecordsRead(1)
        record
      },
      context.taskMetrics().mergeShuffleReadMetrics())

    // An interruptible iterator must be used here in order to support task cancellation
    val interruptibleIter = new InterruptibleIterator[(Any, Any)](context, metricIter)

    val aggregatedIter: Iterator[Product2[K, C]] = if (dep.aggregator.isDefined) {
      if (dep.mapSideCombine) {
        // We are reading values that are already combined
        val combinedKeyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, C)]]
        dep.aggregator.get.combineCombinersByKey(combinedKeyValuesIterator, context)
      } else {
        // We don't know the value type, but also don't care -- the dependency *should*
        // have made sure its compatible w/ this aggregator, which will convert the value
        // type to the combined type C
        val keyValuesIterator = interruptibleIter.asInstanceOf[Iterator[(K, Nothing)]]
        dep.aggregator.get.combineValuesByKey(keyValuesIterator, context)
      }
    } else {
      interruptibleIter.asInstanceOf[Iterator[Product2[K, C]]]
    }

    // Sort the output if there is a sort ordering defined.
    val resultIter = dep.keyOrdering match {
      case Some(keyOrd: Ordering[K]) =>
        // Create an ExternalSorter to sort the data.
        val sorter =
          new ExternalSorter[K, C, C](context, ordering = Some(keyOrd), serializer = dep.serializer)
        sorter.insertAll(aggregatedIter)
        context.taskMetrics().incMemoryBytesSpilled(sorter.memoryBytesSpilled)
        context.taskMetrics().incDiskBytesSpilled(sorter.diskBytesSpilled)
        context.taskMetrics().incPeakExecutionMemory(sorter.peakMemoryUsedBytes)
        // Use completion callback to stop sorter if task was finished/cancelled.
        context.addTaskCompletionListener[Unit](_ => {
          sorter.stop()
        })
        CompletionIterator[Product2[K, C], Iterator[Product2[K, C]]](sorter.iterator, sorter.stop())
      case None =>
        aggregatedIter
    }

    resultIter match {
      case _: InterruptibleIterator[Product2[K, C]] => resultIter
      case _ =>
        // Use another interruptible iterator here to support task cancellation as aggregator
        // or(and) sorter may have consumed previous interruptible iterator.
        new InterruptibleIterator[Product2[K, C]](context, resultIter)
    }
  }
}
