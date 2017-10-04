package com.epam.big_data.lab;

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

public class IPDataProcessingTests {

    private MapDriver<LongWritable, Text, Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;
    private ReduceDriver<Text, Text, Text, Text> combineDriver;
    private MapReduceDriver<LongWritable, Text, Text, Text, Text, Text> mapReduceDriver;

    @Before
    public void setUp() {
        DataMapper mapper = new DataMapper();
        DataReducer reducer = new DataReducer();
        DataCombiner combiner = new DataCombiner();
        mapDriver = MapDriver.newMapDriver(mapper);
        combineDriver = ReduceDriver.newReduceDriver(combiner);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setCombiner(combiner);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testMapper() throws IOException {
        String line = "ip10 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 40028 " +
                "\"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\n";
        mapDriver.withInput(new LongWritable(10), new Text(line));
        mapDriver.withOutput(new Text("ip10"), new Text("40028"));
        mapDriver.runTest();
    }

    @Test
    public void testCombiner() throws IOException {
        String ipName = "ip10";
        List<Text> values = new ArrayList<>();
        Collections.addAll(values, new Text("1000"), new Text("2000"), new Text("300"));
        combineDriver.withInput(new Text(ipName), values);
        combineDriver.withOutput(new Text(ipName), new Text("3300,3"));
        combineDriver.runTest();
    }

    @Test
    public void testReducer() throws IOException {
        String ipName = "ip10";
        List<Text> values = new ArrayList<>();
        Collections.addAll(values, new Text("500,5"), new Text("1000,10"), new Text("300,3"));
        reduceDriver.withInput(new Text(ipName), values);
        reduceDriver.withOutput(new Text("IP_name"), new Text("Avg_bytes Total_bytes"));
        reduceDriver.withOutput(new Text(ipName), new Text(100 + ",00 " + 1800));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws IOException {
        String line = "ip10 - - [24/Apr/2011:04:06:01 -0400] \"GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1\" 200 7200 " +
                "\"-\" \"Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)\n";
        mapReduceDriver.withInput(new LongWritable(10), new Text(line));
        mapReduceDriver.withOutput(new Text("IP_name"), new Text("Avg_bytes Total_bytes"));
        mapReduceDriver.withOutput(new Text("ip10"), new Text(7200 + ",00 " + 7200));
        mapReduceDriver.runTest();

    }
}
