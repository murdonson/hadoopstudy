package com.yihao.hbaseUtil;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by jzy on 2018/11/6.
 */
public class ProductModel implements WritableComparable<ProductModel> {

    @Override
    public int compareTo(ProductModel o) {
        if (this == o) {
            return 0;
        }
        int temp = this.id.compareTo(o.getId());
        if (temp!=0) {
            return temp;
        }
         temp = this.name.compareTo(o.getName());
        if (temp!=0) {
            return temp;
        }
        temp = this.price.compareTo(o.getPrice());
        return temp;


    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(price);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.name=dataInput.readUTF();
        this.price = dataInput.readUTF();
    }

    private String id;
    private String name;
    private String price;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPrice() {
        return price;
    }

    public void setPrice(String price) {
        this.price = price;
    }
}
