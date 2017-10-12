package com.epam.big_data.lab;

import com.epam.big_data.lab.utils.CustomKey;
import com.epam.big_data.lab.utils.GroupComparator;
import com.epam.big_data.lab.utils.KeyPartitioner;
import junit.framework.Assert;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class IPinYouTest {

    private MapDriver<LongWritable, Text, CustomKey, Text> driverForDataMapper;
    private ReduceDriver<CustomKey, Text, CustomKey, Text> driverForCombiner;
    private ReduceDriver<CustomKey, Text, Text, IntWritable> driverForDataReducer;
    private MapReduceDriver<LongWritable, Text, CustomKey, Text, Text, IntWritable> generalMRDriver;

    @Before
    public void setUp() throws URISyntaxException {

        DataMapper dataMapper = new DataMapper();
        GroupComparator groupComparator = new GroupComparator();
        DataCombiner dataCombiner = new DataCombiner();
        DataReducer dataReducer = new DataReducer();

        String mockedPathToDistributedCache1 = "src/main/resources/city.en.txt";
        String mockedPathToDistributedCache2 = "src/main/resources/region.en.txt";

        driverForDataMapper = MapDriver.newMapDriver(dataMapper);
        driverForDataMapper.withCacheFile(new URI(mockedPathToDistributedCache1));
        driverForDataMapper.withCacheFile(new URI(mockedPathToDistributedCache2));

        driverForCombiner = ReduceDriver.newReduceDriver(dataCombiner);
        driverForDataReducer = ReduceDriver.newReduceDriver(dataReducer);
        generalMRDriver = MapReduceDriver.newMapReduceDriver();
        generalMRDriver.setMapper(dataMapper);
        generalMRDriver.setKeyGroupingComparator(groupComparator);
        generalMRDriver.setCombiner(dataCombiner);
        generalMRDriver.setReducer(dataReducer);
        generalMRDriver.withCacheFile(new URI(mockedPathToDistributedCache1));
        generalMRDriver.withCacheFile(new URI(mockedPathToDistributedCache2));
    }

    @Test
    public void partitionerTest() {
        String cityName = "beijing";
        String osType = "This is Windows 7 operating system";
        Text value = new Text();
        CustomKey cKey = new CustomKey(new Text(cityName), new Text(osType));

        KeyPartitioner kp = new KeyPartitioner();

        int testValue1 = kp.getPartition(cKey, value, 5);

        osType = "No, this isn't an Android!";
        cKey = new CustomKey(new Text(cityName), new Text(osType));
        int testValue2 = kp.getPartition(cKey, value, 5);

        osType = "Of course, this is Windows XP.";
        cKey = new CustomKey(new Text(cityName), new Text(osType));
        int testValue3 = kp.getPartition(cKey, value, 5);

        osType = "Is it Mac OS X operating system?";
        cKey = new CustomKey(new Text(cityName), new Text(osType));
        int testValue4 = kp.getPartition(cKey, value, 5);

        osType = "Probably, it is iOS device.";
        cKey = new CustomKey(new Text(cityName), new Text(osType));
        int testValue5 = kp.getPartition(cKey, value, 5);

        Assert.assertEquals(2, testValue1);
        Assert.assertEquals(0, testValue2);
        Assert.assertEquals(1, testValue3);
        Assert.assertEquals(3, testValue4);
        Assert.assertEquals(3, testValue5);
    }

    @Test
    public void dataMapperTest() throws IOException {
        Text text = new Text("2e72d1bd7185fb76d69c852c57436d37\t20131019025500549\t1\tCAD06D3WCtf\tMozilla/4.0 " +
                "(compatible; MSIE 6.0; Windows NT 5.1; SV1)\t113.117.187.*\t216\t234\t2\t" +
                "33235ca84c5fee9254e6512a41b3ad5e\t8bbb5a81cc3d680dd0c27cf4886ddeae\tnull\t3061584349\t728\t90\t" +
                "OtherView\tNa\t5\t7330\t277\t48\tnull\t2259\t10057,13800,13496,10079,10076,10075,10093,10129,10024," +
                "10006,10110,13776,10146,10120,10115,10063");
        driverForDataMapper.withInput(new LongWritable(10), text);
        driverForDataMapper.withOutput(new CustomKey(new Text("zhongshan"), new Text("Windows XP")),
                                                                                                 new Text("1"));
        driverForDataMapper.runTest();
    }

    @Test
    public void dataCombinerTest() throws IOException {
        List<Text> values = new ArrayList<>();
        Collections.addAll(values, new Text("1"), new Text("1"), new Text("1"), new Text("1"),
                new Text("1"), new Text("1"), new Text("1"), new Text("1"));
        driverForCombiner.withInput(new CustomKey(new Text("beijing"), new Text("Windows XP")), values);
        driverForCombiner.withOutput(
                new CustomKey(new Text("beijing"), new Text("")), new Text("8"));
        driverForCombiner.runTest();
    }

    @Test
    public void dataReducerTest() throws IOException {
        List<Text> values = new ArrayList<>();
        Collections.addAll(values, new Text("4"), new Text("5"), new Text("6"), new Text("7"),
                new Text("8"), new Text("9"), new Text("10"), new Text("20"));
        driverForDataReducer.withInput(new CustomKey(new Text("shijiazhuang"), new Text("")), values);
        driverForDataReducer.withOutput(new Text("shijiazhuang"), new IntWritable(69));
        driverForDataReducer.runTest();
    }

    @Test
    public void generalMRTest() throws IOException {
        Text testData = new Text("93074d8125fa8945c5a971c2374e55a8\t20131019161502142\t1\tCAH9FYCtgQf\tMozilla/4.0" +
                " (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 2.0.50727)\t119.145.140.*\t216\t222\t1\t" +
                "20fc675468712705dbf5d3eda94126da\t9c1ecbb8a301d89a8d85436ebf393f7f\tnull\tmm_10982364_973726_8930541\t" +
                "300\t250\tFourthView\tNa\t0\t7323\t294\t201\tnull\t2259\t10057,10059,10083,10102,10024,10006,10110," +
                "10031,10063,10116");
        generalMRDriver.withInput(new LongWritable(5), testData);
        generalMRDriver.withOutput(new Text("foshan"), new IntWritable(1));
        generalMRDriver.runTest();
    }
}
