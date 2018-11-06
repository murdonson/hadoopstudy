package com.yihao.HiveGuli;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;


/**
 * Created by jzy on 2018/11/6.
 */
public class HiveLower extends UDF {

    public Text evaluate(Text text) {
        return new Text(text.toString().toLowerCase());
    }
}
