package com.epam.big_data.lab;

import com.epam.big_data.lab.utils.CustomKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DataReducer extends Reducer<CustomKey, Text, Text, IntWritable> {

    @Override
    protected void reduce(CustomKey key, Iterable<Text> values,
                          Reducer<CustomKey, Text, Text, IntWritable>.Context context)
                                                                             throws IOException, InterruptedException {
        Iterator<Text> itr = values.iterator();
        int sum = 0;
        while (itr.hasNext()){
            sum += Integer.parseInt(itr.next().toString());
        }
        if(sum > 0){
            context.write(new Text(key.getCityOrRegionName()), new IntWritable(sum));
        }

    }
}
