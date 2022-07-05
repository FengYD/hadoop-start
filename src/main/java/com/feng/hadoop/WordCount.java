package com.feng.hadoop;


import com.feng.hadoop.mapper.TokenizerMapper;
import com.feng.hadoop.reducer.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 * @author fengyadong
 * @date 2022/7/4 19:59
 * @description
 */
public class WordCount {

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        //指定本程序的jar包所在的本地路径  把jar包提交到yarn
        job.setJarByClass(WordCount.class);

        /*
         * 告诉框架调用哪一个类
         * 指定本业务job要是用的mapper/Reducer业务类
         */
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);

        //指定最终的输出数据的kv类型, 有时候不须要reduce过程，若是有的话最终输出指的就是指reduce K V类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 指定job的文件输入的原始目录
        // paths指你的待处理文件能够在多个目录里边
        // 第一个参数是你给那个job设置  后边的参数逗号分隔的多个路径 路径是在hdfs里的
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 指定job 的输出结果所在的目录
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        /*
         * 找yarn通讯
         * 将job中配置的参数, 以及job所用的java类所在的jar包提交给yarn去运行
         */
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
