import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.Job;


import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class WordCount{

public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        String line = text.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        while(tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            outputCollector.collect(word,one);
        }
    }
}
    public static void main(String[] args) throws Exception{
//        int exitCode = ToolRunner.run(new WordCount(), args);
//        System.exit(exitCode);
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordCount");
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        JobClient.runJob(conf);
    }


}
 class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    @Override
    public void map(LongWritable longWritable, Text text, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {

        String line = text.toString();
        Log.getLog("line",line,1);
        System.out.println(line);
        StringTokenizer tokenizer = new StringTokenizer(line);
        while(tokenizer.hasMoreTokens()){
            word.set(tokenizer.nextToken());
            outputCollector.collect(word,one);
        }
    }
}

 class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text, IntWritable> {
    @Override
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
        int sum= 0;
        while(iterator.hasNext()){
            sum += iterator.next().get();
        }
        System.out.println(sum);
        outputCollector.collect(text, new IntWritable(sum));
    }
}

//    @Override
//    public int run(String[] strings) throws Exception {
//        if (strings.length != 2) {
//            System.err.printf("Usage: %s needs two arguments, input and outputfiles\n", getClass().getSimpleName());
//            return -1;
//        }
//
//        Job job = new Job();
//        job.setJarByClass(WordCount.class);
//        job.setJobName("WordCounter");
//
//        FileInputFormat.addInputPath(job, new Path(strings[0]));
//        FileOutputFormat.setOutputPath(job, new Path(strings[1]));
//
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(IntWritable.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
//
//        job.setMapperClass(MapClass.class);
//        job.setReducerClass(ReduceClass.class);
//
//        int returnValue = job.waitForCompletion(true) ? 0:1;
//
//        if(job.isSuccessful()) {
//            System.out.println("Job was successful");
//        } else if(!job.isSuccessful()) {
//            System.out.println("Job was not successful");
//        }
//
//        return returnValue;
//    }
