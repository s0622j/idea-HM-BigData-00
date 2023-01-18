package cn.itcast.mapreduce.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @description:  该类就是MapReduce程序客户端驱动类 主要是构造Job对象实例
 *                指定各种组件属性 包括：mapper reducer类、输入输出的数据类型、输入输出的数据路径
 *                提交job作业  job.submit()
 * @author: Itcast
 */
public class WordCountDriver_v1 {
    public static void main(String[] args) throws Exception{

        //创建配置对象
        Configuration conf = new Configuration();

        //设置MapReduce程序的运行模式 如果不指定 默认是local模式
        conf.set("mapreduce.framework.name","local");

        //构建Job作业的实例 参数（配置对象、Job名字）
        Job job = Job.getInstance(conf, WordCountDriver_v1.class.getSimpleName());
        //设置mr程序运行的主类
        job.setJarByClass(WordCountDriver_v1.class);

        //设置本次mr程序的mapper类型  reducer类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        //指定mapper阶段输出的key value数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        //指定reducer阶段输出的key value类型 也是mr程序最终的输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //配置本次作业的输入数据路径 和输出数据路径
        Path input = new Path(args[0]);
        Path output = new Path(args[1]);
        //todo 默认组件 TextInputFormat TextOutputFormat
        FileInputFormat.setInputPaths(job,input);
        FileOutputFormat.setOutputPath(job,output);

        //todo 判断输出路径是否已经存在 如果存在先删除
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output)){
            //fs.delete(output,true);//rm -rf
        }

        //最终提交本次job作业
//        job.submit();
        //采用waitForCompletion提交job 参数表示是否开启实时监视追踪作业的执行情况
        boolean resultflag = job.waitForCompletion(true);
        //退出程序 和job结果进行绑定
        System.exit(resultflag ? 0: 1);
    }
}
