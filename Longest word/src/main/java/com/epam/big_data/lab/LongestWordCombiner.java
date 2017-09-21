package com.epam.big_data.lab;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
  This tool removes duplicates and retains only the longest words for writing to context from input stream after mapper
  processing.
 */

public class LongestWordCombiner extends Reducer<IntWritable, Text, IntWritable, Text> {

    private List<Text> listOfWords = new ArrayList<>();
    private IntWritable wordLengthNegative = new IntWritable();

    private void combine(Iterable<Text> words) throws IOException, InterruptedException{

        Iterator<Text> itr = words.iterator();
        while (itr.hasNext()){
            listOfWords.add(new Text(itr.next()));
        }
        Comparator<Text> comparator = Comparator.comparing(Text::getLength);
        listOfWords.sort(comparator.reversed());
        int maxLength = listOfWords.get(0).getLength();
        Stream<Text> streamOfWords = listOfWords.stream().distinct();
        listOfWords = streamOfWords.filter(word -> word.getLength() == maxLength).collect(Collectors.toList());

    }

    @Override
    public void run(Reducer<IntWritable, Text, IntWritable, Text>.Context context) throws IOException, InterruptedException {

        while(context.nextKey()) {
            combine(context.getValues());
            Iterator<Text> iter = listOfWords.iterator();
            while (iter.hasNext()){
                Text word = new Text(iter.next());
                wordLengthNegative.set(word.getLength()*(-1));
                context.write(wordLengthNegative, word);
            }
            listOfWords.clear();
        }

    }

}
