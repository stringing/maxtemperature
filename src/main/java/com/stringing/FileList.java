package com.stringing;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Description cluster配置测试
 * @Author Stringing
 * @Date 9/10/18 5:56 PM
 */
public class FileList extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        if(strings.length != 2){
            System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.err);
        }
        FileSystem fs = FileSystem.get(getConf());
        for(Path p : FileUtil.stat2Paths(fs.listStatus(new Path(strings[0])))){
            System.out.println(p);
        }
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new FileList(), args);
        System.exit(exitCode);
    }
}
