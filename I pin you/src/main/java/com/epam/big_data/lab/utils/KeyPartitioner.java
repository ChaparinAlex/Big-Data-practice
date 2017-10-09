package com.epam.big_data.lab.utils;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class KeyPartitioner extends Partitioner<CustomKey, Text> {

    @Override
    public int getPartition(CustomKey key, Text value, int numPartitions) {
        return key.getOperatingSystemType().hashCode() % numPartitions;
    }

}
