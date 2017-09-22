package com.epam.big_data.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class LongestWordMapper extends Mapper<LongWritable, Text, IntWritable, Text>{

    private Text word = new Text();
    private IntWritable wordLengthNegative = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

        String delim = "[ .,:;!?+-*/%#~^\\=_<>()[]|\"@¿&“”€0123456789\t\n\f\r$]+";
        StringTokenizer st = new StringTokenizer(value.toString(), delim);

        while (st.hasMoreTokens()){
            word.set(st.nextToken());
            wordLengthNegative.set(word.getLength()*(-1));
            context.write(wordLengthNegative, word);
        }

    }


}
