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


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.eyllo.paprika.test.stress.hadoop.NThreadInputFormat.RangeInputSplit;


/**
 * Treats keys as offset in file and value as line. 
 */
public class ThreadRecordReader extends RecordReader<LongWritable, Text> {
    long start;
    long finish;
    long total;
    long count;
    
    
    LongWritable key = null;
    Text value;

    public ThreadRecordReader() {
    }
    
    public void initialize(InputSplit split, TaskAttemptContext context) 
        throws IOException, InterruptedException {
      start = ((RangeInputSplit)split).first;
      finish = 0;
      count = ((RangeInputSplit)split).count;
      total = ((RangeInputSplit)split).total;
    }

    public void close() throws IOException {
      // NOTHING
    }

    public LongWritable getCurrentKey() {
      return key;
    }

    public Text getCurrentValue() {
      return new Text("");
    }

    public float getProgress() throws IOException {
        if ((start+finish) == total) {
            return 0.0f;
          } else {
   	       return Math.min(1.0f, (finish) / (float)count );
          }
    }

    public boolean nextKeyValue() {
    	value = new Text();
      if (key == null) {
        key = new LongWritable();
      }
      if (finish < count) {
        key.set(start + finish);
        value.set(key.toString());
        finish += 1;
        return true;
      } else {
        return false;
      }
    }
}