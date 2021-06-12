package com.Travis.mapreduce.reduceJoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

public class TableReducer extends Reducer<Text,TableBean,TableBean,NullWritable>{


    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context) throws IOException, InterruptedException {
        // prepara two collection
        ArrayList<TableBean> orderBeans = new ArrayList<> ();
        TableBean pdBean = new TableBean ();

//        circular
        for (TableBean value : values) {
            if("order".equals (value.getFlag ())){
                TableBean tmptableBean = new TableBean ();
                try {
                    BeanUtils.copyProperties (tmptableBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace ();
                } catch (InvocationTargetException e) {
                    e.printStackTrace ();
                }
                orderBeans.add (tmptableBean);
            }else{
                try {
                    BeanUtils.copyProperties(pdBean,value);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }



            }





        }
//          loop  through orderBean and assignment
        for (TableBean orderBean : orderBeans) {
            orderBean.setPname(pdBean.getPname());
            //写出修改后的 orderBean 对象
            context.write(orderBean,NullWritable.get());
        }




    }
}
