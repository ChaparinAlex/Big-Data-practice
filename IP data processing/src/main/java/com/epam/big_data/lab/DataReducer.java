package com.epam.big_data.lab;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;

public class DataReducer extends Reducer<Text, SumAndCountBytesData, Text, Text> {

    @Override
    protected void setup(Reducer<Text, SumAndCountBytesData, Text, Text>.Context context) throws IOException, InterruptedException {
        context.write(new Text("IP_name"), new Text("Avg_bytes Total_bytes"));
    }

    @Override
    protected void reduce(Text key, Iterable<SumAndCountBytesData> values, Context context) throws IOException, InterruptedException {

        long sumOfBytes = 0;
        int quantity = 0;
        NumberFormat formatter = new DecimalFormat("#0.00");
        for(SumAndCountBytesData bytesAndQuantityPair : values){
            sumOfBytes += bytesAndQuantityPair.getSumOfBytes().get();
            quantity += bytesAndQuantityPair.getCountOfRecords().get();
        }
        double averageBytes = (double) sumOfBytes/quantity;
        context.write(new Text(key), new Text(formatter.format(averageBytes) + " " + sumOfBytes));

    }

    @Override
    public void run(Reducer<Text, SumAndCountBytesData, Text, Text>.Context context)
            throws IOException, InterruptedException {

        setup(context);
        while (context.nextKey()){
            reduce(context.getCurrentKey(), context.getValues(), context);
        }

    }

}
