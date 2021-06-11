package com.Travis.mapreduce.writableComparable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 1.定义类实现Writable接口
 * 2.重写序列化和反序列化接口
 * 3.重写空参构造
 * 4.toString方法
 */
public class FlowBean implements WritableComparable<FlowBean>{
    private long upFlow;//上行流量
    private  long downFlow;//下行流量
    private  long sumFlow;//总流量

//空参构造




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
    public void setSumFlow( ) {
        this.sumFlow = this.upFlow + this.downFlow;
    }

    public FlowBean() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong (upFlow);
        dataOutput.writeLong (downFlow);
        dataOutput.writeLong (sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.upFlow = dataInput.readLong ();
        this.downFlow = dataInput.readLong ();
        this.sumFlow= dataInput.readLong ();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" +sumFlow;
    }
    @Override
    public int compareTo(FlowBean o) {
        if(this.sumFlow > o.sumFlow){
            return -1 ;
        }else if(this.sumFlow < o.sumFlow){
            return 1;
        }else{
            return 0;
        }

    }
}
