package com.epam.big_data.lab;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class DataCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int sumOfBytes = 0;
        int quantity = 0;
        for(Text bytesToText : values){
            int bytes;
            try{
                bytes = Integer.parseInt(bytesToText.toString());
            }catch (NumberFormatException e){
                bytes = 0;
            }
            sumOfBytes += bytes;
            quantity++;
        }
        context.write(new Text(key), new Text(sumOfBytes + "," + quantity));
    }
}
