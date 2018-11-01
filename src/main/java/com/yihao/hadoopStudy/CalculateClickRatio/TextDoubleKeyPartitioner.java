package com.yihao.hadoopStudy.CalculateClickRatio;


import com.yihao.hadoopStudy.Po.TextDoublePairOrderDesc;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by jzy on 2018/10/30.
 */
public class TextDoubleKeyPartitioner extends Partitioner<TextDoublePairOrderDesc,Text> {
    @Override
    public int getPartition(TextDoublePairOrderDesc textDoublePairOrderDesc, Text text, int numPartitions) {
        return textDoublePairOrderDesc.getKey().hashCode()%numPartitions;
    }
}
