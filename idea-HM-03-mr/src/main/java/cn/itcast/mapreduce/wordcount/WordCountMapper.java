package cn.itcast.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @description: WordCount Mapper类 对应着Maptask
 * @author: Itcast
 *
 * KEYIN: 表示map阶段输入kv中的k类型    在默认组件下 是起始位置偏移量 因此是LongWritable
 * VALUEIN:表示map阶段输入kv中的v类型   在默认组件下 是每一行内容   因此是Text.
 *      todo MapReduce有默认的读取数据组件  叫做TextInputFormat
 *      todo 读数据的行为是：一行一行读取数据  返回kv键值对
 *          k:每一行的起始位置的偏移量 通常无意义
 *          v:这一行的文本内容
 * KEYOUT:   表示map阶段输出kv中的k类型  跟业务相关 本需求中输出的是单词  因此是Text
 * VALUEOUT: 表示map阶段输出kv中的v类型  跟业务相关 本需求中输出的是单词次数1 因此是LongWritable
 */
public class WordCountMapper extends Mapper<LongWritable, Text,Text,LongWritable> {

    private Text outkey = new Text();
    private final static LongWritable outValue = new LongWritable(1);

    /**
     *  map方法是mapper阶段核心方法 也是具体业务逻辑实现的方法
     *  注意，该方法被调用的次数和输入的kv键值对有关，每当TextInputFormat读取返回一个kv键值对，就调用一次map方法进行业务处理
     *  默认情况下，map方法是基于行来处理数据
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //拿取一行数据转换String
        String line = value.toString();//  hello tom hello allen hello
        //根据分隔符进行切割
        String[] words = line.split("\\s+");// [hello,tom,hello,allen,hello]
        //遍历数组
        for (String word : words) {

            outkey.set(word);
            //输出数据 把每个单词标记1  输出的结果<单词，1>
            //使用上下文对象 将数据输出
            context.write(outkey,outValue);// <hello,1> <tom,1><hello,1> <allen,1><hello,1>
        }
    }
}
