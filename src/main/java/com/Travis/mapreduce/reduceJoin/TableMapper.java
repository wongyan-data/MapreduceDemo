package com.Travis.mapreduce.reduceJoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class TableMapper extends Mapper<LongWritable,Text,Text,TableBean>{

    private String filename;
    private Text outK  = new Text () ;
    private TableBean outV  = new TableBean () ;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // obtain  the Filename 
        InputSplit split = context.getInputSplit ();
        FileSplit fileSplit  =(FileSplit)split;
        filename = fileSplit.getPath ().getName ();

    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//    obtain one line data
        String  line =  value.toString ();
//       judge the file path
        if(filename.contains ("order")){
            String[] split = line.split ("\t");
//            packaging  outK;
            outK.set (split[1]);
//            packaging  outV;
            outV.setId (split[0]);
            outV.setPid (split[1]);
            outV.setAmount (Integer.parseInt (split[2]));
            outV.setPname ("");
            outV.setFlag ("order");
        }else{
            String[] split = line.split("\t");
            //封装 outK
            outK.set(split[0]);
            //封装 outV
            outV.setId("");
            outV.setPid(split[0]);
            outV.setAmount(0);
            outV.setPname(split[1]);

            outV.setFlag("pd");
        }
//        write  out KV
        context.write (outK ,outV);
        }




    }

