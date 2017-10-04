package com.epam.big_data.lab;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DataMapper extends Mapper<LongWritable, Text, Text, Text>{

    private Text ipName = new Text();
    private Text bytesPerIP = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        StringTokenizer st = new StringTokenizer(value.toString(), "\n");

        while (st.hasMoreTokens()){
            String line = st.nextToken().trim();
            String[] sections = line.split(" ");
            ipName.set(new Text(sections[0]));
            bytesPerIP.set(new Text(sections[9]));
            context.write(ipName, bytesPerIP);
        }

    }


}
