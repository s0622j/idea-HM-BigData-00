package cn.itcast.sentiment_upload.task;

import cn.itcast.sentiment_upload.arg.SentimentOptions;
import cn.itcast.sentiment_upload.dfs.HDFSMgr;
import cn.itcast.sentiment_upload.dfs.HDFSMgrImpl;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 任务操作
 */
public class TaskMgr {
    protected static Logger Logger = LogManager.getLogger(TaskMgr.class.getName());

    protected static final String COPY_STATUS = "_COPY";
    protected static final String DONE_STATUS = "_DONE";

    private HDFSMgr hdfsUtil;

    public TaskMgr() {
        hdfsUtil = new HDFSMgrImpl();
    }

    /**
     *  生成待上传目录
         1.判断原始数据目录是否存在
         2.读取原始数据目录下的所有文件
         3.判断待上传目录是否存在，不存在则创建一个
         4.创建任务目录（目录名称：task_年月日时分秒_任务状态）
         5.遍历待上传的文件，在待上传目录生成一个willDoing文件
         6.将待移动的文件添加到willDoing文件中
     */
    public void genTask(SentimentOptions options) {
        // 判断原始数据目录是否存在
        File sourceDir = new File(options.sourceDir);
        if(!sourceDir.exists()) {
            String errorMsg = String.format("%s 要采集的原始数据目录不存在.", options.sourceDir);
            Logger.error(errorMsg);
            throw new RuntimeException(errorMsg);
        }

        // 读取原始数据目录下的所有文件
        File[] allSourceDataFile = sourceDir.listFiles(f -> {
            // 判断文件格式是否以 weibo_data_ 开头
            String fileName = f.getName();
            if (fileName.startsWith("weibo_data_")) {
                return true;
            }

            return false;
        });

        // 判断待上传目录是否存在，不存在则创建一个
        File tempDir = new File(options.pendingDir);
        if(!tempDir.exists()) {
            try {
                FileUtils.forceMkdirParent(tempDir);
            } catch (IOException e) {
                Logger.error(e.getMessage(), e);
                throw new RuntimeException(e.getMessage());
            }
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        StringBuilder stringBuilder = new StringBuilder();

        // 创建任务目录（目录名称：task_年月日时分秒_任务状态
        File taskDir = null;
        // 判断数据文件是否为空
        if(allSourceDataFile != null && allSourceDataFile.length > 0) {
            taskDir = new File(tempDir, String.format("task_%s", sdf.format(new Date())));
            taskDir.mkdir();
        }
        else {
            return;
        }

        // 遍历待上传的文件
        // 在待上传目录生成一个willDoing文件
        for (File dataFile : allSourceDataFile) {
            try {
                File destFile = new File(taskDir, dataFile.getName());
                FileUtils.moveFile(dataFile, destFile);
                // 将文件的绝对路径保存下来
                stringBuilder.append(destFile.getAbsoluteFile() + "\n");
            } catch (IOException e) {
                Logger.error(e.getMessage(), e);
            }
        }

        // 将待移动的文件添加到willDoing文件中
        try {
            String taskName = String.format("willDoing_%s", sdf.format(new Date()));
            FileUtils.writeStringToFile(new File(tempDir, taskName)
                    , stringBuilder.toString()
                    , "utf-8");
        } catch (IOException e) {
            Logger.error(e.getMessage(), e);
        }
    }

    /**
     * 处理任务
     *      a)将任务文件修改为_COPY，表示正在处理中
     * 	    b)获取任务的日期
     * 	    c)判断HDFS目标上传目录是否存在，不存在则创建
     * 	    d)读取任务文件
     * 	    e)按照换行符切分
     * 	    f)上传每一个文件,调用HDFSUtils进行数据文件上传
     * 	    g)上传成功后，将_COPY后缀修改为_DONE
     */
    public void work(SentimentOptions options) {
        // 3.1 读取待上传目录的willDoing任务文件，注意过滤COPY和DONE后的任务文件夹
        File pendingDir = new File(options.pendingDir);
        File[] pendingTaskDir = pendingDir.listFiles(f -> {
            String taskName = f.getName();
            // 文件是以willDoing开头的
            if(!taskName.startsWith("willDoing"))  return false;

            if (taskName.endsWith(COPY_STATUS) || taskName.endsWith(DONE_STATUS)) {
                return false;
            } else {
                return true;
            }
        });

        // 3.2 遍历读取任务文件，开始上传
        for (File pendingTask : pendingTaskDir) {
            try {
                // 将任务文件修改为_COPY，表示正在处理中
                File copyTaskFile = new File(pendingTask.getAbsolutePath() + "_" + COPY_STATUS);
                FileUtils.moveFile(pendingTask, copyTaskFile);

                // 获取任务的日期
                String taskDate = pendingTask.getName().split("_")[1];
                String dataPathInHDFS = options.output + String.format("/%s", taskDate);
                // 判断HDFS目标上传目录是否存在，不存在则创建
                hdfsUtil.mkdir(dataPathInHDFS);

                // 读取任务文件
                String tasks = FileUtils.readFileToString(copyTaskFile, "utf-8");
                // 按照换行符切分
                String[] taskArray = tasks.split("\n");

                // 上传每一个文件
                for (String task : taskArray) {
                    // 调用HDFSUtils进行数据文件上传
                    hdfsUtil.put(task, dataPathInHDFS);
                }

                // 上传成功后，将_COPY后缀修改为_DONE
                File doneTaskFile = new File(pendingTask.getAbsolutePath() + "_" + DONE_STATUS);
                FileUtils.moveFile(copyTaskFile, doneTaskFile);
            } catch (IOException e) {
                Logger.error(e.getMessage(), e);
            }

        }

    }
}
