package com.epam.training.bigdata;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserAgentMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

}
