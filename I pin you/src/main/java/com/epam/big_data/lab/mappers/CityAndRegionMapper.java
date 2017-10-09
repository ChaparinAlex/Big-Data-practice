package com.epam.big_data.lab.mappers;

import com.epam.big_data.lab.utils.CustomKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CityAndRegionMapper extends Mapper<LongWritable, Text, CustomKey, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CustomKey, Text>.Context context)
                                                                              throws IOException, InterruptedException{

        StringTokenizer st = new StringTokenizer(value.toString(), "\n");

        while (st.hasMoreTokens()){
            String line = st.nextToken().trim();
            String[] sections = line.split("\t");
            if(sections.length == 2){
                context.write(new CustomKey(sections[0], ""), new Text(sections[1]));
            }
        }

    }

}
