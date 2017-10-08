package com.epam.big_data.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DataReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    private Text cityName = new Text();

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Reducer<IntWritable, Text, Text, IntWritable>.Context context)
                                                                             throws IOException, InterruptedException {
        Iterator<Text> itr = values.iterator();
        int quantity = 0;
        Integer currentValue;
        while (itr.hasNext()){
            Text value = new Text(itr.next());
            try{
                currentValue = Integer.parseInt(value.toString());
            }catch (NumberFormatException e){
                e.printStackTrace();
                currentValue = null;
            }
            if(currentValue == null){
                cityName.set(value);
            }else{
                quantity++;
            }
        }
        context.write(cityName, new IntWritable(quantity));

    }
}
