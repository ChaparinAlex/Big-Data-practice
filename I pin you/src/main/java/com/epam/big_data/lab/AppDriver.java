package com.epam.big_data.lab;

import com.epam.big_data.lab.utils.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;
import java.util.List;

public class AppDriver extends Configured implements Tool {

    private static final String HDFS_RELATIVE_INPUT_PATH = "i_pin_you/input";
    private static final String HDFS_RELATIVE_OUTPUT_PATH = "i_pin_you/output";
    private static final String HDFS_DCACHE_RELATIVE_PATH = "i_pin_you/dcache";
    private static String hdfsInputFolder;
    private static String hdfsOutputFolder;
    private static String hdfsDCacheFolder;
    private HDFSOperations hdfsOperations;


    public static void main(String[] args) throws Exception {
        if (args.length != 0 && args.length != 3) {
            System.out.println("To run the program please use following number of arguments: \n0 - " +
                    "your HDFS input, output and distributed cache directories will be created automatically and they " +
                    "will obtain the next paths: <hdfsHomeFolder>/i_pin_you/input, <hdfsHomeFolder>/i_pin_you/output and" +
                    "<hdfsHomeFolder>/i_pin_you/dcache;" +
                    "\n3 (i/o HDFS-paths and distributed cache path) - you must type input, output and distributed cache " +
                    "HDFS directories manually");
            System.out.println("Your current arguments: ");
            for (String arg : args) {
                System.out.println(arg);
            }
            System.out.println("Please, restart with proper number of arguments!");
            return;
        }
        int res = ToolRunner.run(new AppDriver(), args);
        System.out.println("Results of MapReduce task (first 5 files; if files aren't exist it should be empty):");
        for(int i = 0; i < 5; i++){
            System.out.println("\nResults from file " + i + ":");
            FileSystemOperations.executeCommand("hdfs dfs -cat " + hdfsOutputFolder + "/part-r-0000" + i);
        }
        System.exit(res);
    }

    public int run(String[] args) throws Exception {

        Configuration conf = getConf();
        hdfsOperations = new HDFSOperations(conf);

        if(args.length == 3){
            hdfsInputFolder = args[0];
            hdfsOutputFolder = args[1];
            hdfsDCacheFolder = args[2];
        }else{
            hdfsInputFolder = hdfsOperations.getHdfsHomeFolder() + "/" + HDFS_RELATIVE_INPUT_PATH;
            hdfsOutputFolder = hdfsOperations.getHdfsHomeFolder() + "/" + HDFS_RELATIVE_OUTPUT_PATH;
            hdfsDCacheFolder = hdfsOperations.getHdfsHomeFolder() + "/" + HDFS_DCACHE_RELATIVE_PATH;
        }

        hdfsOperations.setUp(new Path(hdfsInputFolder), new Path(hdfsOutputFolder), new Path(hdfsDCacheFolder));

        Job job = Job.getInstance(conf);
        job.setJarByClass(getClass());
        job.setJobName("I pin you");

        FileInputFormat.addInputPath(job, new Path(hdfsInputFolder));
        FileOutputFormat.setOutputPath(job, new Path(hdfsOutputFolder));

        job.setMapperClass(DataMapper.class);

        job.setMapOutputKeyClass(CustomKey.class);
        job.setMapOutputValueClass(Text.class);

        job.setCombinerClass(DataCombiner.class);
        job.setReducerClass(DataReducer.class);

        job.setPartitionerClass(KeyPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);

        //adding files from hdfsDCacheFolder to set of distributed cache files
        List<Path> dCacheFiles = hdfsOperations.getAllFilesFromDirectory(new Path(hdfsDCacheFolder));
        for(Path path : dCacheFiles){
            job.addCacheFile(new URI(path.toString()));
        }

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(5);

        return job.waitForCompletion(true) ? 0 : 1;

    }
}

