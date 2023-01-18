package cn.itcast.hdfs.sequence;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

/**
 * @description:
 * @author: Allen Woon
 * @time: 2020/12/23 20:27
 */
public class SequenceFileRead {
    public static void main(String[] args) throws IOException {
        //设置客户端运行身份 以root去操作访问HDFS
        System.setProperty("HADOOP_USER_NAME","root");
        //Configuration 用于指定相关参数属性
        Configuration conf = new Configuration();

        //SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(new Path("hdfs://node1:8020/seq.out"));
        SequenceFile.Reader.Option option1 = SequenceFile.Reader.file(new Path("hdfs://mycluster:8020/seq.out"));
        SequenceFile.Reader.Option option2 = SequenceFile.Reader.length(174);//这个参数表示读取的长度

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(conf,option1,option2);
            Writable key = (Writable) ReflectionUtils.newInstance(
                    reader.getKeyClass(), conf);
            Writable value = (Writable) ReflectionUtils.newInstance(
                    reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
                String syncSeen = reader.syncSeen() ? "*" : "";//是否返回了Sync Mark同步标记
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

}
