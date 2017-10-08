package com.epam.big_data.lab.mappers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DataMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, Text>.Context context)
                                                                              throws IOException, InterruptedException{

        StringTokenizer st = new StringTokenizer(value.toString(), "\n");

        while (st.hasMoreTokens()){
            String line = st.nextToken().trim();
            String[] sections = line.split("\t");
            Long biddingPrice = null;
            try{
                biddingPrice = Long.parseLong(sections[19]);
            }catch (NumberFormatException e){
                e.printStackTrace();
            }
            if(biddingPrice == null || biddingPrice <= 250){
                continue;
            }
            Integer cityId = null;
            try{
                cityId = Integer.parseInt(sections[7]);
            }catch (NumberFormatException e){
                e.printStackTrace();
            }
            if(cityId != null){
                context.write(new IntWritable(cityId), new Text("1"));
            }else{
                Integer regionId;
                try{
                    regionId = Integer.parseInt(sections[6]);
                }catch (NumberFormatException e){
                    e.printStackTrace();
                    regionId = 0;
                }
                context.write(new IntWritable(regionId), new Text("1"));
            }

        }

    }


}
