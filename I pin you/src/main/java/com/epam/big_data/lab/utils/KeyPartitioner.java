package com.epam.big_data.lab.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<CustomKey, Text> {

    @Override
    public int getPartition(CustomKey key, Text value, int numPartitions) {

        String osType = key.getOperatingSystemType().toString();
        if(osType == null){
            return (key.getCityOrRegionName().hashCode() & Integer.MAX_VALUE) % numPartitions;
        }
        if(osType.contains("Android")) {
            return 0;
        }
        if(osType.contains("Windows XP")){
            return 1;
        }
        if(osType.contains("Windows 7")){
            return 2;
        }
        if(osType.contains("Mac OS X") || osType.contains("iOS")){
            return 3;
        }
        return 4;

    }

}
