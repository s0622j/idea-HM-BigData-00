package cn.itcast.sentiment_upload.dfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HDFS操作类
 */
public class HDFSMgrImpl implements HDFSMgr {

    // log4j配置文件
    protected static Logger Logger = LogManager.getLogger(HDFSMgrImpl.class.getName());

    private Configuration configuration;
    private FileSystem fileSystem;

    public HDFSMgrImpl() {
        try {
            configuration = new Configuration();
            fileSystem = FileSystem.get(configuration);
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 读文件
     * @param path
     * @param recursion 是否递归读取
     * @return
     */
    @Override
    public List<String> ls(String path, boolean recursion) {
        try {
            // 指定遍历某个HDFS路径
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path(path), recursion);
            ArrayList<String> fileList = new ArrayList<>();

            while(iterator.hasNext()) {
                LocatedFileStatus fileStatus = iterator.next();
                // 获取文件的路径
                fileList.add(fileStatus.getPath().toString());
            }

            return fileList;
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 上传文件到HDFS
     */
    @Override
    public void put(String src, String dest) {
        try {
            // 从本地文件中上传文件到HDFS
            fileSystem.copyFromLocalFile(false, true, new Path(src), new Path(dest));
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void get(String src, String destLocal) {
        try {
            fileSystem.copyToLocalFile(new Path(src), new Path(destLocal));
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 创建文件夹
     */
    @Override
    public void mkdir(String path) {
        try {
            // 判断文件夹是否存在
            if (fileSystem.exists(new Path(path))) {
                return;
            }

            // 在HDFS中创建目录
            fileSystem.mkdirs(new Path(path));
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 关闭文件系统
     */
    @Override
    public void close() {
        try {
            fileSystem.close();
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
