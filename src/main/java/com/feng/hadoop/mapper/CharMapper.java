package com.feng.hadoop.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author fengyadong
 * @date 2022/7/5 19:46
 * @description
 */
public class CharMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text character = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        char[] characters = line.toCharArray();
        for (char ch : characters) {
            character.set(String.valueOf(ch));
            context.write(character, one);
        }
    }
}