package com.Travis.mapreduce.reduceJoin;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class TableBean implements Writable{

    private String id; //订单详情
    private String pid;//产品id
    private int  amount;//产品数量
    private String pname ;
    private String flag ;

    public TableBean() {
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getPid() {
        return pid;
    }

    public void setPid(String pid) {
        this.pid = pid;
    }

    public int getAmount() {
        return amount;
    }

    public void setAmount(int amount) {
        this.amount = amount;
    }

    public String getPname() {
        return pname;
    }

    public void setPname(String pname) {
        this.pname = pname;
    }

    public String getFlag() {
        return flag;
    }

    public void setFlag(String flag) {
        this.flag = flag;
    }

    @Override
    public String toString() {
        return id + "\t" + pname + "\t" + amount;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF (id);
        dataOutput.writeUTF (pid);
        dataOutput.writeInt (amount);
        dataOutput.writeUTF (pname);
        dataOutput.writeUTF (flag);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
    this.id = dataInput.readUTF ();
    this.pid = dataInput.readUTF ();
    this.amount = dataInput.readInt ();
    this.pname = dataInput.readUTF ();
    this.flag = dataInput.readUTF ();

    }
}
