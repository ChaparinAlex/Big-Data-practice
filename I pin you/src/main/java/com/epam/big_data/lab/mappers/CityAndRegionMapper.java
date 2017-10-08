package com.epam.big_data.lab.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class CityAndRegionMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                                                                              throws IOException, InterruptedException{

        StringTokenizer st = new StringTokenizer(value.toString(), "\n");

        while (st.hasMoreTokens()){
            String line = st.nextToken().trim();
            String[] sections = line.split("\t");
            Integer id;
            try{
                id = Integer.parseInt(sections[0]);
            }catch (NumberFormatException e){
                e.printStackTrace();
                id = 0;
            }
            context.write(new IntWritable(id), new Text(sections[1]));
        }

    }

}
