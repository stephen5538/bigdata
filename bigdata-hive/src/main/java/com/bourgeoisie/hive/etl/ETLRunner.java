package com.bourgeoisie.hive.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class ETLRunner implements Tool {
    private Configuration conf = null;

    public void setConf(Configuration conf) {
        this.conf = conf;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public int run(String[] args) throws Exception {
        conf = this.getConf();
//        conf.set("inpath", args[0]);
        conf.set("inpath", "C:\\Users\\zhangxw\\Desktop\\笔记\\hive笔记\\综合练习数据\\gulivideo\\video\\2008\\0222");
        conf.set("outpath", "C:\\Users\\zhangxw\\Desktop\\笔记\\hive笔记\\综合练习数据\\gulivideo\\user1");
        Job job = Job.getInstance(conf);
        job.setJarByClass(ETLRunner.class);

        job.setMapperClass(ETLMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        this.initJobInputPath(job);
        this.initJobOutputPath(job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void initJobOutputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String outPathString = conf.get("outpath");

        FileSystem fs = FileSystem.get(conf);

        Path outPath = new Path(outPathString);
        if (fs.exists(outPath)) {
            fs.delete(outPath, true);
        }

        FileOutputFormat.setOutputPath(job, outPath);

    }

    private void initJobInputPath(Job job) throws IOException {
        Configuration conf = job.getConfiguration();
        String inPathString = conf.get("inpath");

        FileSystem fs = FileSystem.get(conf);

        Path inPath = new Path(inPathString);
        if (fs.exists(inPath)) {
            FileInputFormat.addInputPath(job, inPath);
        } else {
            throw new RuntimeException("HDFS中该文件目录不存在：" + inPathString);
        }
    }

    public static void main(String[] args) {
        try {
            int resultCode = ToolRunner.run(new ETLRunner(), args);
            if (resultCode == 0) {
                System.out.println("Success!");
            } else {
                System.out.println("Fail!");
            }
            System.exit(resultCode);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
