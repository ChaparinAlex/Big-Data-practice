package com.epam.big_data.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class LongestWordReducer extends Reducer<IntWritable, Text, List, IntWritable> {

    private List<Text> listOfWords = new ArrayList<>();
    private IntWritable lengthOfWord = new IntWritable();

    @Override
    protected void reduce(IntWritable wordLengthNegative, Iterable<Text> words,
                          Reducer<IntWritable, Text, List, IntWritable>.Context context)
            throws IOException, InterruptedException{

        lengthOfWord.set(wordLengthNegative.get()*(-1));
        Iterator<Text> itr = words.iterator();
        while (itr.hasNext()){
            Text word = new Text(itr.next());
            listOfWords.add(word);
        }
        Stream<Text> streamOfWords = listOfWords.stream().distinct();
        listOfWords = streamOfWords.collect(Collectors.toList());
        context.write(listOfWords, lengthOfWord);

    }

    @Override
    public void run(Reducer<IntWritable, Text, List, IntWritable>.Context context) throws IOException, InterruptedException {

        context.nextKey();
        reduce(context.getCurrentKey(), context.getValues(), context);
        listOfWords.clear();

    }


}
