package com.epam.big_data.lab.mappers;

import com.epam.big_data.lab.utils.CustomKey;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DataMapper extends Mapper<LongWritable, Text, CustomKey, Text>{

    @Override
    protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, CustomKey, Text>.Context context)
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
            String operatingSystemName = UserAgent.parseUserAgentString(sections[4]).getOperatingSystem().getName();
            String cityId = sections[7];
            if(!cityId.equals("null")){
                context.write(new CustomKey(cityId, operatingSystemName), new Text("1"));
            }else{
                String regionId = sections[6];
                context.write(new CustomKey(regionId, operatingSystemName), new Text("1"));
            }

        }

    }


}
