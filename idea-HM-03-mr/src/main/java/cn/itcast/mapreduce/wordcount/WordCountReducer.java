package cn.itcast.mapreduce.wordcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @description: 本类就是MapReduce程序中Reduce阶段的处理类 对应着ReduceTask
 * @author: Itcast
 *
 * KEYIN: 表示的是reduce阶段输入kv中k的类型 对应着map的输出的key 因此本需求中 就是单词 Text
 * VALUEIN:表示的是reduce阶段输入kv中v的类型 对应着map的输出的value 因此本需求中 就是单词次数1 LongWritable
 * KEYOUT: 表示的是reduce阶段输出kv中k的类型  跟业务相关 本需求中 还是单词 Text
 * VALUEOUT:表示的是reduce阶段输出kv中v的类型  跟业务相关 本需求中 还是单词总次数 LongWritable
 */
public class WordCountReducer extends Reducer<Text, LongWritable,Text,LongWritable> {


    private LongWritable outValue =new LongWritable();

    /**
     * todo Q：当map的所有输出数据来到reduce之后 该如何调用reduce方法进行处理呢？
     *      <hello,1><hadoop,1><hello,1><hello,1><hadoop,1>
     * 1、排序  规则：根据key的字典序进行排序 a-z
     *      <hadoop,1><hadoop,1><hadoop,1><hello,1><hello,1>
     * 2、分组  规则：key相同的分为一组
     *      <hadoop,1><hadoop,1><hadoop,1>
     *      <hello,1><hello,1>
     * 3、分组之后，同一组的数据组成一个新的kv键值对，调用一次reduce方法。     reduce方法基于分组调用的 一个分组调用一次。
     *      todo 同一组中数据组成一个新的kv键值对。
     *      新key:该组共同的key
     *      新value:该组所有的value组成的一个迭代器Iterable
     *      <hadoop,1><hadoop,1><hadoop,1>----><hadoop,Iterable[1,1,1]>
     *      <hello,1><hello,1>----> <hello,Iterable[1,1]>
     */
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        //统计变量
        long count = 0;
        //遍历该组的values
        for (LongWritable value : values) {//<hadoop,Iterable[1,1,1]>
            //累加计算总次数
            count += value.get();
        }
        outValue.set(count);
        //最终使用上下文对象输出结果
        context.write(key,outValue);
    }
}
