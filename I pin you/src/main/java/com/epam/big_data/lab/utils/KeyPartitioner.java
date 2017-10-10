package com.epam.big_data.lab.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<CustomKey, Text> {

    @Override
    public int getPartition(CustomKey key, Text value, int numPartitions) {

        String valueData = value.toString();
        if(valueData.contains("+")){
            if(valueData.contains("Android")) {
                return 0;
            }
            if(valueData.contains("Windows XP")){
                return 1;
            }
            if(valueData.contains("Windows 7")){
                return 2;
            }
            if(valueData.contains("Mac OS X") || valueData.contains("iOS")){
                return 3;
            }
            return 4;
        }
        return (value.hashCode() & Integer.MAX_VALUE) % numPartitions;

    }

}
