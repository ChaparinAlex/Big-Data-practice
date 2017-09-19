package com.epam.big_data.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class LongestWordReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    private List<Text> collectionOfMaxLengthWords;
    private IntWritable lengthOfWord;

    @Override
    protected void reduce(IntWritable wordLength, Iterable<Text> words, Context context)
            throws IOException, InterruptedException{

        lengthOfWord = wordLength;
        collectionOfMaxLengthWords = new ArrayList<>();
        Iterator<Text> itr = words.iterator();
        while (itr.hasNext()){
            collectionOfMaxLengthWords.add(itr.next());
        }

    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Text word : collectionOfMaxLengthWords){
            context.write(word, lengthOfWord);
        }
    }


}
