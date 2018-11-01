package com.yihao.hadoopStudy.CalculateClickRatio;


import com.yihao.hadoopStudy.Po.UserClickLog;
import com.yihao.hadoopStudy.Po.UserShowLog;
import com.yihao.hadoopStudy.config.Config;
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
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Created by jzy on 2018/10/30.
 *   计算出针对每个商品的点击率
 */
public class CalculateClickRatio {

    public static class ClickMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private UserClickLog userClickLog = new UserClickLog();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                userClickLog.parse(value.toString());
            } catch (UserClickLog.ClickErrorException e) {
                context.getCounter("ClickMapper","ClickErrorException click data not enough").increment(1);
                return;
            }

            if (!userClickLog.getOsKey().equals(Config.KAISHU_OSKEY)) {
                context.getCounter("ClickMapper", "not CalculateClickRatio log");
                return;
            }

            String mid = userClickLog.getMid();
            String algGroup = userClickLog.getAlgGroup();
            outKey.set(mid+Config.KEY_DELIMITER+algGroup);
            outValue.set("click"+Config.KEY_DELIMITER+1);
            context.getCounter("ClickMapper", "Out count").increment(1);
            context.write(outKey,outValue);

        }

    }


    public static class ShowMapper extends Mapper<Object, Text, Text, Text> {
        private Text outKey = new Text();
        private Text outValue = new Text();
        private UserShowLog userShowLog = new UserShowLog();

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            try {
                userShowLog.parse(value.toString());
            } catch (UserShowLog.ShowErrorException e) {
                context.getCounter("ShowMapper","ClickErrorException data not enough").increment(1);
                return;
            }

            if (!userShowLog.getOsKey().equals(Config.KAISHU_OSKEY)) {
                context.getCounter("ShowMapper", "not CalculateClickRatio log");
                return;
            }

            String mid = userShowLog.getMid();

            String algGroup = userShowLog.getAlgGroup();
            outKey.set(mid+Config.KEY_DELIMITER+algGroup);
            outValue.set("show"+Config.KEY_DELIMITER+1);
            context.getCounter("ShowMapper", "Out count").increment(1);
            context.write(outKey,outValue);

        }
    }


    public static class CalculateReducer extends Reducer<Text, Text, Text, Text> {

        private Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double click=0;
            double show=0;
            for (Text value : values) {
                String line = value.toString();
                String[] split = line.split("::");
                if (split[0].equals("click")) {
                    click += Double.parseDouble(split[1]);
                }
                if (split[0].equals("show")) {
                    show += Double.parseDouble(split[1]);
                }
            }


            if (show == 0) {
                context.getCounter("CalculateReducer", "No Show But Click").increment(1);
                return;
            }

            double ration = 0;
            try {
                ration = click /(show);
            } catch (ArithmeticException e) {
                context.getCounter("CalculateReducer", "show is zero").increment(1);
                return;
            }
            outValue.set(ration+"");
            context.write(key,outValue);
        }
    }

    // clickDir showDir DesDir days
    public static void main(String[] args) throws Exception {


        FileUtils.deleteDirectory(new File(args[2]));


        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "CalculateActiveUserRatio");
        job.setJarByClass(CalculateClickRatio.class);
        job.setReducerClass(CalculateReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);

        int days = Integer.parseInt(args[3]);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String curr = sdf.format(System.currentTimeMillis());

        for (; days > 0; days--) {
            MultipleInputs.addInputPath(job, new Path(args[0] + File.separator + curr), TextInputFormat.class,ClickMapper.class);
            MultipleInputs.addInputPath(job, new Path(args[1] + File.separator + curr), TextInputFormat.class,ShowMapper.class);
            Date date = sdf.parse(curr);
            curr = sdf.format(date.getTime()- 24 * 3600 * 1000L);
        }
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }





}
