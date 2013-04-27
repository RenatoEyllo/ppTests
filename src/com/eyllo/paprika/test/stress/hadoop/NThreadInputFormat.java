package com.eyllo.paprika.test.stress.hadoop;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;


public class NThreadInputFormat extends InputFormat<LongWritable, Text> {
	 public static final String NUM_PROCESS = "mapreduce.process.num";
	 public static final String NUM_SPLITS = "mapreduce.job.maps";
	  /**
     * An input split consisting of a range on numbers.
     */
    static class RangeInputSplit extends InputSplit implements Writable {
      long first;
      long count;
      long total;

      public RangeInputSplit() { }

      public RangeInputSplit(long offset, long length, long total) {
        first = offset;
        count = length;
        this.total = total;
      }

      public long getLength() throws IOException {
        return 0;
      }

      public String[] getLocations() throws IOException {
        return new String[]{};
      }

      public void readFields(DataInput in) throws IOException {
        first = WritableUtils.readVLong(in);
        count = WritableUtils.readVLong(in);
        total = WritableUtils.readVLong(in);
      }

      public void write(DataOutput out) throws IOException {
        WritableUtils.writeVLong(out, first);
        WritableUtils.writeVLong(out, count);
        WritableUtils.writeVLong(out, total);
      }
    }
    /**
     * A record reader that will generate a range of numbers.
     */

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit arg0, TaskAttemptContext arg1) throws IOException,
			InterruptedException {
	      return new ThreadRecordReader();
	}
	@Override
	public List<InputSplit> getSplits(JobContext job) throws IOException,
			InterruptedException {
	      long totalThreads = getNumberOfProcess(job);
	      int numSplits = job.getConfiguration().getInt(NUM_SPLITS, 1);
	   	      List<InputSplit> splits = new ArrayList<InputSplit>();
	      long currentThread = 0;
	      for(int split = 0; split < numSplits; ++split) {
	        long goal =   (long) Math.ceil(totalThreads * (double)(split + 1) / numSplits);
	        splits.add(new RangeInputSplit(currentThread, goal - currentThread, totalThreads));
	        currentThread = goal;
	      }
	      return splits;
	}
	  
	static long getNumberOfProcess(JobContext job) {
	    return job.getConfiguration().getLong(NUM_PROCESS, 0);
	}
	  
	static void setNumberOfProcess(Job job, long numThreads) {
	    job.getConfiguration().setLong(NUM_PROCESS, numThreads);
	}
	  
	static long getNumberOfSplits(JobContext job) {
		return job.getConfiguration().getLong(NUM_SPLITS, 0);
	 }
		  
	static void setNumberOfSplits(Job job, long numSplits) {
		job.getConfiguration().setLong(NUM_SPLITS, numSplits);
	}
}