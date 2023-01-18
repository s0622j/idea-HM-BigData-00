package cn.itcast.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * @description:
 * @author: Allen Woon
 * @time: 2021/1/8 17:31
 */
public class HDFSClientTest {

    private static Configuration conf =null;
    private static FileSystem fs =null;

    /**
     * 初始化方法 用于和hdfs集群建立连接
     * @throws IOException
     */
    @Before
    public void connect2HDFS() throws IOException {
        //设置客户端身份 以具备权限在hdfs上进行操作
        System.setProperty("HADOOP_USER_NAME","root");
        //创建配置对象实例
        conf = new Configuration();
        //设置操作的文件系统是HDFS 并且指定HDFS操作地址
        conf.set("fs.defaultFS","hdfs://node1:8020");
        //创建FileSystem对象实例
        fs = FileSystem.get(conf);
    }

    /**
     * 创建文件夹操作
     */
    @Test
    public void mkdir() throws IOException {
        //首先判断文件夹是否存在，如果不存在再创建
        if(!fs.exists(new Path("/itheima"))){
            //创建文件夹
            fs.mkdirs(new Path("/ithiema"));
        }
    }


    /**
     * 上传文件
     */
    @Test
    public void putFile2HDFS() throws IOException {
        //创建本地文件路径
        Path src = new Path("D:\\hello.txt");
        //hdfs上传路径
        Path dst = new Path("/ithiema/1.txt");
        //文件上传动作(local--->hdfs)
        fs.copyFromLocalFile(src,dst);
    }

    /**
     * 下载文件
     */
    @Test
    public void getFile2Local() throws IOException {
        //源路径：hdfs的路径
        Path src = new Path("/ithiema/1.txt");
        //目标路径：local本地路径
        Path dst = new Path("D:\\1.txt");
        //文件下载动作(hdfs--->local)
        fs.copyToLocalFile(src,dst);
    }



    /**
     * 关闭客户端和hdfs连接
     * @throws IOException
     */
    @After
    public void close() {
        //首先判断文件系统实例是否为null 如果不为null 进行关闭
        if(fs !=null){
            try {
                fs.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
