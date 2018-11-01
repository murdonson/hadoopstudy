package com.yihao.hadoopStudy.FlowBean;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by jzy on 2018/10/26.
 */
public class FlowSort {

    public static class FlowSortMapper extends Mapper<Object, Text, FlowBean, NullWritable> {


        private FlowBean outKey = new FlowBean();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String regex = "\\s+";
            String[] split = line.split(regex);
            String phoneNumber = "";
            long upFlow=0;
            long downFlow=0;
            try {
                phoneNumber = split[1];
                upFlow= Long.parseLong(split[8]);
                downFlow = Long.parseLong(split[9]);
            } catch (Exception e) {
                context.getCounter("FlowSort","splitException").increment(1);
                return;
            }
            outKey.set(phoneNumber, upFlow, downFlow);
            context.write(outKey,NullWritable.get());
        }
    }


    public static class FlowSortReducer extends Reducer<FlowBean, NullWritable, Text, FlowBean> {
        private Text outKey = new Text();
        // 这里针对一个reduceTask
        @Override
        protected void reduce(FlowBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            // 这里只针对同一个 partitioner
            String phoneNumber = key.getPhoneNumber();
            outKey.set(phoneNumber);
            context.write(outKey,key);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileUtils.deleteDirectory(new File("E:\\IdeaProjects\\hadoopstudy\\data\\flowdata\\results"));
        Job job = Job.getInstance(configuration, "flowSort");
        job.setJarByClass(FlowSort.class);
        job.setReducerClass(FlowSort.FlowSortReducer.class);
        job.setMapOutputKeyClass(FlowBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputKeyClass(FlowBean.class);

        job.setNumReduceTasks(1);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,FlowSort.FlowSortMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }


}
