package com.yihao.HiveGuli.Etl;

import com.yihao.HiveGuli.util.CleanUtil;
import com.yihao.hadoopStudy.FlowBean.FlowBean;
import com.yihao.hadoopStudy.FlowBean.FlowSort;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

/**
 * Created by jzy on 2018/11/1.
 */
public class Etl {

    // 一个文件小于128m就对应一个mapper  大于128m就对应多个mapper
    public static class EtlMapper extends Mapper<Object, Text, Text, NullWritable> {

        private Text outKey = new Text();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String clean = CleanUtil.clean(line);
            if (clean == null) {
                context.getCounter("EtlMapper", "input fields < 10").increment(1);
                return;
            }

            outKey.set(clean);
            context.write(outKey,NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        FileUtils.deleteDirectory(new File("E:\\IdeaProjects\\hadoopstudy\\data\\hiveguli\\results1"));
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, "etl");
        job.setJarByClass(FlowSort.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
        MultipleInputs.addInputPath(job,new Path(args[0]), TextInputFormat.class,EtlMapper.class);
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }


}
