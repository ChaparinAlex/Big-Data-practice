package com.epam.big_data.lab;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class LongestWordTest {

    private MapDriver<LongWritable, Text, IntWritable, Text> driverForMapper;
    private ReduceDriver<IntWritable, Text, Text, IntWritable> driverForReducer;
    private ReduceDriver<IntWritable, Text, IntWritable, Text> driverForCombiner;
    private MapReduceDriver<LongWritable, Text, IntWritable, Text, Text, IntWritable> generalMRDriver;

    @Before
    public void setUp() {
        LongestWordMapper mapper = new LongestWordMapper();
        LongestWordReducer reducer = new LongestWordReducer();
        LongestWordCombiner combiner = new LongestWordCombiner();
        driverForMapper = MapDriver.newMapDriver(mapper);
        driverForReducer = ReduceDriver.newReduceDriver(reducer);
        generalMRDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
        driverForCombiner =  ReduceDriver.newReduceDriver(combiner);
    }

    @Test
    public void combinerTest() throws IOException {
        List<Text> words = new ArrayList<>();
        Collections.addAll(words, new Text("MapReduce"), new Text("MRUnit"), new Text("BigData"));
        driverForCombiner.withInput(new IntWritable(10), words);
        driverForCombiner.withOutput(new IntWritable("MapReduce".length()*(-1)), new Text("MapReduce"));
        driverForCombiner.runTest();
    }

    @Test
    public void mapperTest() throws IOException {
        Text text = new Text("  ^.,:;!?+-*/%#~^\\ElasticSearch=_<>()[]| \"@¿096\n&“”€$");
        driverForMapper.withInput(new LongWritable(10), text);
        driverForMapper.withOutput(new IntWritable("ElasticSearch".length()*(-1)), new Text("ElasticSearch"));
        driverForMapper.runTest();
    }

    @Test
    public void reducerTest() throws IOException {
        List<Text> words = new ArrayList<>();
        Collections.addAll(words, new Text("Flink"), new Text("Oozie"), new Text("Spark"),
                new Text("Oozie"));
        driverForReducer.withInput(new IntWritable(-5), words);
        driverForReducer.withOutput(new Text("Flink, Oozie, Spark"), new IntWritable(5));
        driverForReducer.runTest();
    }

    @Test
    public void generalMRTest() throws IOException {
        generalMRDriver.withInput(new LongWritable(10), new Text("Flink Hive Oozie HBase Spark Oozie Pig"));
        generalMRDriver.withOutput(new Text("Flink, Oozie, HBase, Spark"), new IntWritable(5));
        generalMRDriver.runTest();
    }
}
