package com.epam.big_data.lab;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class DataMapper extends Mapper<LongWritable, Text, Text, SumAndCountBytesData>{

    private Text ipName = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        StringTokenizer st = new StringTokenizer(value.toString(), "\n");

        while (st.hasMoreTokens()){
            String line = st.nextToken().trim();
            String[] sections = line.split(" ");
            ipName.set(new Text(sections[0]));
            long totalBytes;
            try{
                totalBytes = Long.parseLong(sections[9]);
            }catch(NumberFormatException e){
                totalBytes = 0;
            }
            StringBuilder sb = new StringBuilder();
            for(int i = 11; i < sections.length; i++){
                sb.append(sections[i]);
                sb.append(" ");
            }
            String userAgentData = sb.toString().trim();
            String browserName = UserAgent.parseUserAgentString(userAgentData).getBrowser().getName();
            if(browserName != null){
                context.getCounter("Browsers", browserName).increment(1);
            }
            context.write(ipName, new SumAndCountBytesData(totalBytes, 1));
        }

    }


}
