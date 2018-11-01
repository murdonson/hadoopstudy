package com.yihao.hadoopStudy.Po;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jzy on 2018/10/30.
 */
public class TextDoublePairOrderDesc implements WritableComparable<TextDoublePairOrderDesc> {

    private String key;
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public TextDoublePairOrderDesc() {
    }

    @Override
    public int compareTo(TextDoublePairOrderDesc that) {
        if (!this.getKey().equals(that.getKey())) {
            return this.getKey().compareTo(that.getKey());
        }

        return -this.getValue().compareTo(that.getValue());

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(key);
        dataOutput.writeUTF(value);

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.key = dataInput.readUTF();
        this.value = dataInput.readUTF();

    }



}
