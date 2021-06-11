package com.Travis.mapreduce.writableComprablePartitioner;

import com.Travis.mapreduce.partitioner2.FlowBean;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class ProvincePartitioner2 extends Partitioner<com.Travis.mapreduce.writableComprablePartitioner.FlowBean,Text>{


    @Override
    public int getPartition(com.Travis.mapreduce.writableComprablePartitioner.FlowBean flowBean, Text text, int i) {
//        获取手机号后三位
        String phone =   text.toString ();
        String prephone =   phone.substring (0,3);

//        定义分区号变量partition，根据prephone设置分区号
        int  partition ;
        if("136".equals (prephone)){
            partition = 0 ;
        }else if("137".equals (prephone)){
            partition =1 ;
        }else if("138".equals(prephone)){
            partition = 2;
        }else if("139".equals(prephone)){
            partition = 3;
        }else {
            partition = 4;
        }
        return partition;
    }
}
