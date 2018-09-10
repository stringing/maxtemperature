package com.stringing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * @Description 用MRUnit来测试
 * @Author Stringing
 * @Date 9/5/18 4:32 PM
 */

public class MaxTemperatureTest {

    //对mapper的测试
    @Test
    public void processesValidRecord() throws IOException {
        Text value = new Text("0029029070999991901010106004+64333+023450FM-12+" +
                "000599999V0202701N015919999999N0000001N9-00781-00789102001ADDGF108991999999999999999999");
        new MapDriver<LongWritable,Text,Text,IntWritable>()
                .withMapper(new MaxTemperatureMapper())
                .withInput(new LongWritable(0), value)
                //若是缺失值+9999则没有输出,没有输出的不用withOutput
                .withOutput(new Text("1901"), new IntWritable(-78))
                .runTest();
    }

    //对reducer的测试
    @Test
    public void returnsMaximumIntegerInValues() throws IOException {
        new ReduceDriver<Text,IntWritable,Text,IntWritable>()
                .withReducer(new MaxTemperatureReducer())
                .withInput(new Text("1901"), Arrays.asList(new IntWritable(10), new IntWritable(5)))
                .withOutput(new Text("1901"), new IntWritable(10))
                .runTest();
    }

    //测试驱动程序
    @Test
    public void testDriver() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapreduce.framework.name", "local");
        //map的缓存大小(MB)
        conf.setInt("mapreduce.task.io.sort.mb", 1);

        Path input = new Path("/home/luowenqiang/development/hadoop_app/1901_1902");
        Path output = new Path("/tmp/output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        MaxTemperatureDriver driver = new MaxTemperatureDriver();
        driver.setConf(conf);
        driver.run(new String[]{input.toString(), output.toString()});

        //也可以启动一个mini集群来测试
        //hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-*-tests.jar minicluster

    }
}
