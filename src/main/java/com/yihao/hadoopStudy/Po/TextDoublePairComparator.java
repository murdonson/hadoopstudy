package com.yihao.hadoopStudy.Po;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by jzy on 2018/11/1.
 */
public class TextDoublePairComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // TODO Auto-generated method stub
        TextDoublePairOrderDesc tp1 = (TextDoublePairOrderDesc) a;
        TextDoublePairOrderDesc tp2 = (TextDoublePairOrderDesc) b;
        return tp1.getKey().compareTo(tp2.getKey());
    }

    public TextDoublePairComparator() {
        super(TextDoublePairOrderDesc.class,true);
    }
}
