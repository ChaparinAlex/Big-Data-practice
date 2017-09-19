package com.epam.big_data.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class LongestWordMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

    private Text word = new Text();
    private IntWritable wordLength;

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        StringTokenizer itr = new StringTokenizer(value.toString());

        while (itr.hasMoreTokens()){
            word.set(itr.nextToken());
            wordLength = new IntWritable(word.getLength());
            context.write(wordLength, word);
        }

    }


}
