package cn.itcast.hdfs.sequence;

/**
 * @description:
 * @author: Allen Woon
 * @time: 2020/12/24 11:21
 */
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeSmallFilesToSequenceFile {
    private Configuration configuration = new Configuration();
    private List<String> smallFilePaths = new ArrayList<String>();


    //定义方法用来添加小文件的路径
    public void addInputPath(String inputPath) throws Exception{
        File file = new File(inputPath);
        //给定路径是文件夹，则遍历文件夹，将子文件夹中的文件都放入smallFilePaths
        //给定路径是文件，则把文件的路径放入smallFilePaths
        if(file.isDirectory()){
            File[] files = FileUtil.listFiles(file);
            for(File sFile:files){
                smallFilePaths.add(sFile.getPath());
                System.out.println("添加小文件路径：" + sFile.getPath());
            }
        }else{
            smallFilePaths.add(file.getPath());
            System.out.println("添加小文件路径：" + file.getPath());
        }
    }
    //把smallFilePaths的小文件遍历读取，然后放入合并的sequencefile容器中
    public void mergeFile() throws Exception{
        Writer.Option bigFile = Writer.file(new Path("D:\\bigfile.seq"));
        Writer.Option keyClass = Writer.keyClass(Text.class);
        Writer.Option valueClass = Writer.valueClass(BytesWritable.class);
        //构造writer
        Writer writer = SequenceFile.createWriter(configuration, bigFile, keyClass, valueClass);
        //遍历读取小文件，逐个写入sequencefile
        Text key = new Text();
        for(String path:smallFilePaths){
            File file = new File(path);
            long fileSize = file.length();//获取文件的字节数大小
            byte[] fileContent = new byte[(int)fileSize];
            FileInputStream inputStream = new FileInputStream(file);
            inputStream.read(fileContent, 0, (int)fileSize);//把文件的二进制流加载到fileContent字节数组中去
            String md5Str = DigestUtils.md5Hex(fileContent);
            System.out.println("merge小文件："+path+",md5:"+md5Str);
            key.set(path);
            //把文件路径作为key，文件内容做为value，放入到sequencefile中
            writer.append(key, new BytesWritable(fileContent));
        }
        writer.hflush();
        writer.close();
    }
    //读取大文件中的小文件
    public void readMergedFile() throws Exception{
        Reader.Option file = Reader.file(new Path("D:\\bigfile.seq"));
        Reader reader = new Reader(configuration, file);
        Text key = new Text();
        BytesWritable value = new BytesWritable();
        while(reader.next(key, value)){
            byte[] bytes = value.copyBytes();
            String md5 = DigestUtils.md5Hex(bytes);
            String content = new String(bytes, Charset.forName("GBK"));
            System.out.println("读取到文件："+key+",md5:"+md5+",content:"+content);
        }
    }

    public static void main(String[] args) throws Exception {
        MergeSmallFilesToSequenceFile msf = new MergeSmallFilesToSequenceFile();
        //合并小文件
//		msf.addInputPath("D:\\datasets\\smallfile");
//		msf.mergeFile();
        //读取大文件
        msf.readMergedFile();
    }
}
