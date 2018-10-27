package com.yihao.hadoopStudy.InvertedIndex;

import com.yihao.hadoopStudy.FlowBean.FlowBean;
import com.yihao.hadoopStudy.FlowBean.FlowSort;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;

/**
 * Created by jzy on 2018/10/26.
 */
public class InvertedIndexStep1 {


    public static String getFilePath(Mapper.Context context) throws Exception {
        // 抽象类
        InputSplit inputSplit = context.getInputSplit();
        Class<? extends InputSplit> splitClass = inputSplit.getClass();
        Method getInputSplit = splitClass.getDeclaredMethod("getInputSplit");
        getInputSplit.setAccessible(true);
        FileSplit fileSplit= (FileSplit) getInputSplit.invoke(inputSplit);
        return fileSplit.getPath().getName();
    }


    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String filePath = null;
            try {
                filePath = getFilePath(context);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            String[] split = value.toString().split("\\s+");
            for (int i = 0; i < split.length; i++) {
                outKey.set(split[i]+"::"+filePath);
                outValue.set("1");
                context.write(outKey,outValue);
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();

        //  dj::a.txt,3
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count=0;
            for (Text value : values) {
                String line = value.toString();
                int i = Integer.parseInt(line);
                count+=i;
            }
            outValue.set(String.valueOf(count));
            context.write(key,outValue);

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileUtils.deleteDirectory(new File("E:\\IdeaProjects\\hadoopstudy\\data\\invertedIndex2\\results"));
        Job job = Job.getInstance(configuration, "invertedIndexStep1");
        job.setJarByClass(InvertedIndexStep1.class);
        job.setReducerClass(InvertedIndexReducer.class);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(Text.class);

        job.setNumReduceTasks(1);

        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,InvertedIndexMapper.class);
        MultipleInputs.addInputPath(job,new Path(args[1]), TextInputFormat.class,InvertedIndexMapper.class);


        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
