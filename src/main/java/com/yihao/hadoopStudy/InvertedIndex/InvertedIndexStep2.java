package com.yihao.hadoopStudy.InvertedIndex;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jzy on 2018/10/27.
 */
public class InvertedIndexStep2 {


    /**
     * aa::a.txt	1
     * dj::a.txt	1
     * dj::b.txt	1
     */
    public static class InvertedIndexStep2Mapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String word = line.split("::")[0];
            String fileNameAndCount = line.split("::")[1];
            outKey.set(word);
            outValue.set(fileNameAndCount);
            context.write(outKey,outValue);
        }
    }

    public static class InvertedIndexStep2Reducer extends Reducer<Text, Text, Text, Text> {

        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder sb = new StringBuilder();

            List<String> list = new ArrayList<>();
            for (Text value : values) {
                String line = value.toString();
                list.add(line);
            }

            Collections.sort(list,(left,right)->{
                int countLeft = Integer.parseInt(left.split("\\s+")[1]);
                int countRight = Integer.parseInt(right.split("\\s+")[1]);
                return countLeft>countRight?-1:1;
            });

            list.forEach(e->sb.append(e+"\t"));
            outValue.set(sb.toString());
            context.write(key,outValue);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileUtils.deleteDirectory(new File("E:\\IdeaProjects\\hadoopstudy\\data\\invertedIndexresults2"));
        Job job = Job.getInstance(configuration, "invertedIndexStep2");
        job.setJarByClass(InvertedIndexStep2.class);
        job.setReducerClass(InvertedIndexStep2Reducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);

        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,InvertedIndexStep2Mapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }





}
