package com.epam.big_data.lab;

import com.epam.big_data.lab.utils.CustomKey;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class DataCombiner extends Reducer<CustomKey, Text, CustomKey, Text> {

    @Override
    protected void reduce(CustomKey key, Iterable<Text> values, Reducer<CustomKey, Text, CustomKey, Text>.Context context)
            throws IOException, InterruptedException {
        Iterator<Text> itr = values.iterator();
        int quantity = 0;
        while (itr.hasNext()){
            itr.next();
            quantity++;
        }
        context.write(new CustomKey(new Text(key.getCityOrRegionName()), new Text("")),
                                                                                       new Text(quantity + ""));

    }

}
