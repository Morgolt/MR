package com.epam.training.bigdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserAgentReducer extends Reducer<Text, LongWritable, NullWritable, Text> {

}
