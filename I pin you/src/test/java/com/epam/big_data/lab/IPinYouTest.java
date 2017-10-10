package com.epam.big_data.lab;

import com.epam.big_data.lab.mappers.CityAndRegionMapper;
import com.epam.big_data.lab.mappers.DataMapper;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;


public class IPinYouTest {

    private MapDriver<LongWritable, Text, CustomKey, Text> driverForDataMapper;
    private MapDriver<LongWritable, Text, CustomKey, Text> driverForCityAndRegionMapper;
    private GroupComparator groupComparator;
    private ReduceDriver<CustomKey, Text, Text, IntWritable> driverForDataReducer;
    private MapReduceDriver<LongWritable, Text, CustomKey, Text, Text, IntWritable> generalMRDriver;

    @Before
    public void setUp() {
        DataMapper dataMapper = new DataMapper();
        CityAndRegionMapper crMapper = new CityAndRegionMapper();
        groupComparator = new GroupComparator();
        DataReducer dataReducer = new DataReducer();
        driverForDataMapper = MapDriver.newMapDriver(dataMapper);
        driverForCityAndRegionMapper = MapDriver.newMapDriver(crMapper);
        driverForDataReducer = ReduceDriver.newReduceDriver(dataReducer);
        generalMRDriver = MapReduceDriver.newMapReduceDriver();
        generalMRDriver.setMapper(dataMapper);
        generalMRDriver.setMapper(crMapper);
        generalMRDriver.setKeyGroupingComparator(groupComparator);
        generalMRDriver.setReducer(dataReducer);
    }

    @Test
    public void partitionerTest() {
        Text value = new Text("This is Windows 7 operating system+");
        CustomKey cKey = new CustomKey();

        KeyPartitioner kp = new KeyPartitioner();

        int testValue1 = kp.getPartition(cKey, value, 5);

        value.set("No, this isn't an Android!+");
        int testValue2 = kp.getPartition(cKey, value, 5);

        value.set("Of course, this is Windows XP.+");
        int testValue3 = kp.getPartition(cKey, value, 5);

        value.set("Is it Mac OS X operating system?+");
        int testValue4 = kp.getPartition(cKey, value, 5);

        value.set("Probably, it is iOS device.+");
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
        driverForDataMapper.withOutput(new CustomKey("234"), new Text("Windows XP+"));
        driverForDataMapper.runTest();
    }

    @Test
    public void cityAndRegionMapperTest() throws IOException {
        Text text = new Text("4\tshijiazhuang");
        driverForCityAndRegionMapper.withInput(new LongWritable(10), text);
        driverForCityAndRegionMapper.withOutput(new CustomKey("4"), new Text("shijiazhuang"));
        driverForCityAndRegionMapper.runTest();
    }

    @Test
    public void dataReducerTest() throws IOException {
        List<Text> values = new ArrayList<>();
        Collections.addAll(values, new Text("Windows XP+"), new Text("shijiazhuang"),
                new Text("Mac OS X+"), new Text("Android+"), new Text("Windows 7+"),
                new Text("Android+"), new Text("Mac OS X+"), new Text("Windows XP+"));
        driverForDataReducer.withInput(new CustomKey("4"), values);
        driverForDataReducer.withOutput(new Text("shijiazhuang"), new IntWritable(7));
        driverForDataReducer.runTest();
    }

    @Test
    public void generalMRTest() throws IOException {
        Text testData = new Text("4\tshijiazhuang");
        generalMRDriver.withInput(new LongWritable(5), testData);
        generalMRDriver.withOutput(new Text("shijiazhuang"), new IntWritable(0));
        generalMRDriver.runTest();
    }
}
