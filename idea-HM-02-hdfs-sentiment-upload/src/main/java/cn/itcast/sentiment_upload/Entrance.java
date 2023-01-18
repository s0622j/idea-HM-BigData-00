package cn.itcast.sentiment_upload;

import cn.itcast.sentiment_upload.arg.SentimentOptions;
import cn.itcast.sentiment_upload.task.TaskMgr;
import com.google.devtools.common.options.OptionsParser;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collections;

/**
 * 应用程序入口
 *
 * 1. Google Options:https://github.com/pcj/google-options
 *
 * 2. Log4j
 */
public class Entrance {

    // 创建一个Logger
    protected static final Logger Logger = LogManager.getLogger(Entrance.class.getName());

    public static void main(String[] args) {

        // 解析命令行参数
        OptionsParser parser = OptionsParser.newOptionsParser(SentimentOptions.class);
        parser.parseAndExitUponError(args);
        SentimentOptions options = parser.getOptions(SentimentOptions.class);

        // 判断参数如果为空，则打印帮助信息
        if (options.sourceDir.isEmpty() || options.output .isEmpty()) {
            printUsage(parser);
            return;
        }

        Logger.info("舆情上报程序启动...");

        TaskMgr taskMgr = new TaskMgr();
        // 1. 生成上传任务
        Logger.info("正在生成上传任务...");
        taskMgr.genTask(options);
        // 2. 执行上传任务
        Logger.info("正在上报数据到HFDS");
        taskMgr.work(options);
        Logger.info("DONE");

    }

    private static void printUsage(OptionsParser parser) {
        System.out.println("Usage: java -jar sentiment.jar OPTIONS");
        System.out.println(parser.describeOptions(Collections.<String, String>emptyMap(),
                OptionsParser.HelpVerbosity.LONG));
    }
}
