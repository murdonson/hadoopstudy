package com.yihao.hadoopStudy.CalculateClickRatio;


import com.yihao.hadoopStudy.Po.TextDoublePairComparator;
import com.yihao.hadoopStudy.Po.TextDoublePairOrderDesc;
import com.yihao.hadoopStudy.config.Config;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.SimpleDateFormat;

/**
 * Created by jzy on 2018/10/30.
 */
public class SortArticleByWeight {

    public static class sortMapper extends Mapper<Object, Text, TextDoublePairOrderDesc, Text> {
     // AOTYzNjg5Mjc3MjA::default_feed	0.0
        TextDoublePairOrderDesc outKey = new TextDoublePairOrderDesc();
        Text outValue = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(Config.KEY_DELIMITER);
            String mid = split[0];
            String algAndRatio = split[1];
            String regex = "\\t";
            String[] split1 = algAndRatio.split(regex);
            String algGroup = split1[0];
            String ratio = split1[1];

            outKey.setKey(algGroup);  // 用来分区
            outKey.setValue(ratio); // 用来排序


            outValue.set(mid+ratio);
            context.write(outKey,outValue);
        }
    }

    public static class sortReducer extends Reducer<TextDoublePairOrderDesc, Text, Text, Text> {

        private Text outKey = new Text();
        private Text outValue = new Text();
        private int hotlimit=0;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            hotlimit = context.getConfiguration().getInt("hotlimit", 50);
        }

        @Override
        protected void reduce(TextDoublePairOrderDesc key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 只针对一个key  或者说只针对一个组 在这个业务里 针对一个算法组

            int count=0;
            StringBuilder sb = new StringBuilder();

            for (Text value : values) {
                String line = value.toString();
                sb.append(Config.KEY_DELIMITER).append(line);
                count++;
                if (count >= hotlimit) {
                    break;
                }
            }

            if (sb.length() > Config.KEY_DELIMITER.length()) {
                String substring = sb.substring(Config.KEY_DELIMITER.length());
                outKey.set(key.getKey());
                outValue.set(substring);
                context.getCounter("SorArticleByWeightReducer", "out count").increment(1);
                context.write(outKey,outValue);
            } else {
                context.getCounter("SorArticleByWeightReducer", "no hot article").increment(1);
            }


        }
    }


    public static void main(String[] args) throws Exception {

        // win 测试
       // FileUtils.deleteDirectory(new File(args[1]));



        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "SortArticleByWeight");

        conf.setInt("hotlimit",Integer.parseInt(args[2]));
        job.setJarByClass(SortArticleByWeight.class);
        job.setReducerClass(sortReducer.class);

        job.setMapOutputKeyClass(TextDoublePairOrderDesc.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setGroupingComparatorClass(TextDoublePairComparator.class);
        job.setPartitionerClass(TextDoubleKeyPartitioner.class);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        String curr = sdf.format(System.currentTimeMillis());
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, sortMapper.class);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }



}
