package com.bourgeoisie.hive.etl;

import com.bourgeoisie.hive.utils.ETLUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ETLMapper extends Mapper<LongWritable, Text, String, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String result = ETLUtil.parseString(line);
        if (result == null) {
            return;
        }
        context.write(result, NullWritable.get());
    }
}
