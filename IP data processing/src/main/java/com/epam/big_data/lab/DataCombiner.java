package com.epam.big_data.lab;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataCombiner extends Reducer<Text, SumAndCountBytesData, Text, SumAndCountBytesData> {

    @Override
    protected void reduce(Text key, Iterable<SumAndCountBytesData> values, Context context) throws IOException, InterruptedException {

        long sumOfBytes = 0;
        int quantity = 0;
        for(SumAndCountBytesData bytesInfo : values){
            long bytes = bytesInfo.getSumOfBytes().get();
            sumOfBytes += bytes;
            quantity++;
        }
        context.write(new Text(key), new SumAndCountBytesData(sumOfBytes, quantity));

    }
}
