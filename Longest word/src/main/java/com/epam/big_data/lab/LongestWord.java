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

    private static final String HDFS_RELATIVE_INPUT_PATH = "longest_word/input";
    private static final String HDFS_RELATIVE_OUTPUT_PATH = "longest_word/output";


    public static void main(String[] args) throws Exception {
        FileSystemOperations.writeLogsToFile();
        if(args.length != 0 && args.length != 2){
            System.out.println("To run the program please use following number of arguments: \n0 - " +
                    "your HDFS input and output directories will be created automatically and they will obtain the next " +
                    "paths: <hdfsHomeFolder>/longest_word/input and <hdfsHomeFolder>/longest_word/output;" +
                    "\n2 (i/o HDFS-paths) - you must type input and output HDFS directories manually");
            System.out.println("Your current arguments: ");
            for(String arg : args){
                System.out.println(arg);
            }
            System.out.println("Please, restart with proper number of arguments!");
            return;
        }
        int res = ToolRunner.run(new LongestWord(), args);
        String command;
        if(args.length == 0){
            command = "hdfs dfs -cat " + HDFS_RELATIVE_OUTPUT_PATH + "/part-r*";
        }else{
            command = "hdfs dfs -cat " + args[1] + "/part-r*";
        }
        FileSystemOperations.executeCommand(command);
        System.setOut(FileSystemOperations.getConsole());
        System.out.println("Results of MapReduce task (content of file 'console_logs.log'):");
        FileSystemOperations.executeCommand("cat console_logs.log");
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        HDFSOperations hdfsOperations = new HDFSOperations(conf);
        String hdfsInputFolder, hdfsOutputFolder;

        if(args.length == 2){
            hdfsInputFolder = args[0];
            hdfsOutputFolder = args[1];
        }else{
            hdfsInputFolder = hdfsOperations.getHdfsHomeFolder() + "/" + HDFS_RELATIVE_INPUT_PATH;
            hdfsOutputFolder = hdfsOperations.getHdfsHomeFolder() + "/" + HDFS_RELATIVE_OUTPUT_PATH;
        }

        hdfsOperations.setUp(new Path(hdfsInputFolder), new Path(hdfsOutputFolder), ".txt", ".sh");

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        FileInputFormat.addInputPath(job, new Path(hdfsInputFolder));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFolder));

        job.setMapperClass(LongestWordMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(LongestWordCombiner.class);

        job.setReducerClass(LongestWordReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }
}
