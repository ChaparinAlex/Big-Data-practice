package com.epam.big_data.lab;

import com.epam.big_data.lab.utils.FileSystemOperations;
import com.epam.big_data.lab.utils.HDFSOperations;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class LongestWord extends Configured implements Tool {

    private static final String HDFS_RELATIVE_INPUT_PATH = "/longest_word/input";
    private static final String HDFS_RELATIVE_OUTPUT_PATH = "/longest_word/output";

    public static void main(String[] args) throws Exception {
        FileSystemOperations.writeLogsToFile();
        if(args.length != 1 && args.length != 3){
            System.out.println("To run the program please use following number of arguments: \n1 (only main class) - " +
                    "your HDFS input and output directories will be created automatically and they will obtain the next " +
                    "paths: <hdfsHomeFolder>/longest_word/input and <hdfsHomeFolder>/longest_word/output;" +
                    "\n3 (main class + i/o paths) - you must type input and output HDFS directories manually");
            System.out.println("Your current arguments: ");
            for(String arg : args){
                System.out.println(arg);
            }
            System.out.println("Please, restart with proper number of arguments!");
            return;
        }
        int res = ToolRunner.run(new LongestWord(), args);
        String command;
        if(args.length == 1){
            command = "hdfs dfs -cat " + HDFS_RELATIVE_OUTPUT_PATH + "/part-r*";
        }else{
            command = "hdfs dfs -cat " + args[2] + "/part-r*";
        }
        FileSystemOperations.executeCommand(command);
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        HDFSOperations hdfsOperations = new HDFSOperations(conf);
        String hdfsInputFolder, hdfsOutputFolder;

        if(args.length == 3){
            hdfsInputFolder = args[1];
            hdfsOutputFolder = args[2];
        }else{
            hdfsInputFolder = hdfsOperations.getHdfsHomeFolder() + HDFS_RELATIVE_INPUT_PATH;
            hdfsOutputFolder = hdfsOperations.getHdfsHomeFolder() + HDFS_RELATIVE_OUTPUT_PATH;
        }

        hdfsOperations.setUp(".txt", new Path(hdfsInputFolder), new Path(hdfsOutputFolder));

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(hdfsInputFolder));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFolder));

        job.setMapperClass(LongestWordMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(LongestWordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
