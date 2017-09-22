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

public class LongestWordReducer extends Reducer<IntWritable, Text, Text, IntWritable> {

    private List<Text> listOfWords = new ArrayList<>();
    private IntWritable lengthOfWord = new IntWritable();

    @Override
    protected void reduce(IntWritable wordLengthNegative, Iterable<Text> words,
                          Reducer<IntWritable, Text, Text, IntWritable>.Context context)
            throws IOException, InterruptedException{

        lengthOfWord.set(wordLengthNegative.get()*(-1));
        Iterator<Text> itr = words.iterator();
        while (itr.hasNext()){
            Text word = new Text(itr.next());
            listOfWords.add(word);
        }
        Stream<Text> streamOfWords = listOfWords.stream().distinct();
        listOfWords = streamOfWords.collect(Collectors.toList());
        StringBuilder sb = new StringBuilder();
        for(Text word : listOfWords){
            sb.append(", ").append(word.toString());
        }
        sb.delete(0, 2);
        context.write(new Text(sb.toString()), lengthOfWord);

    }

    @Override
    public void run(Reducer<IntWritable, Text, Text, IntWritable>.Context context)
                                                                           throws IOException, InterruptedException {

       context.nextKey();
       reduce(context.getCurrentKey(), context.getValues(), context);
       listOfWords.clear();


    }


}
