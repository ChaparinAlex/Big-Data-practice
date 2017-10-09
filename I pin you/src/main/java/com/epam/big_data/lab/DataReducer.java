package com.epam.big_data.lab;

import com.epam.big_data.lab.utils.CustomKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DataReducer extends Reducer<CustomKey, Text, Text, IntWritable> {

    private Text cityName = new Text();

    @Override
    protected void reduce(CustomKey key, Iterable<Text> values, Reducer<CustomKey, Text, Text, IntWritable>.Context context)
                                                                             throws IOException, InterruptedException {
        Iterator<Text> itr = values.iterator();
        int quantity = 0;
        while (itr.hasNext()){
            Text value = new Text(itr.next());
            if(value.getLength() > 1){
                cityName.set(value);
            }else{
                quantity++;
            }
        }
        context.write(cityName, new IntWritable(quantity));

    }
}
