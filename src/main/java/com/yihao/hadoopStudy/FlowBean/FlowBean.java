package com.yihao.hadoopStudy.FlowBean;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jzy on 2018/10/26.
 */
public class FlowBean implements WritableComparable<FlowBean> {


    public FlowBean() {
    }

    public FlowBean(long downFlow, long upFlow, String phoneNumber) {
        this.downFlow = downFlow;
        this.upFlow = upFlow;
        this.phoneNumber = phoneNumber;
        this.sumFlow=upFlow+downFlow;
    }

    /**
     *   this.sumFlow指定参数  返回-1 指定参数要靠前 在这里 总流量大的靠前
     * @param o 方法参数
     * @return
     */
    @Override
    public int compareTo(FlowBean o) {
        return this.sumFlow>o.sumFlow?-1:1;
    }

    /**
     *  从磁盘到内存
     * @param dataOutput
     * @throws IOException
     */
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(phoneNumber);
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    /**
     *  序列化 从内存到磁盘
     * @param dataInput
     * @throws IOException
     */
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        phoneNumber=dataInput.readUTF();
        upFlow=dataInput.readLong();
        downFlow=dataInput.readLong();
        sumFlow=dataInput.readLong();

    }





    private long upFlow;
    private long downFlow;
    private long sumFlow;
    private String phoneNumber;

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    public String getPhoneNumber() {
        return phoneNumber;
    }

    public void setPhoneNumber(String phoneNumber) {
        this.phoneNumber = phoneNumber;
    }

    public void set(String phoneNumber, long upFlow, long downFlow) {
        this.phoneNumber=phoneNumber;
        this.upFlow=upFlow;
        this.downFlow=downFlow;
        this.sumFlow=upFlow+downFlow;
    }

    @Override
    public String toString() {
       return "\t"+upFlow+"\t"+downFlow+"\t"+sumFlow;
    }
}
