package cn.itcast.sentiment_upload.arg;

import com.google.devtools.common.options.Option;
import com.google.devtools.common.options.OptionsBase;

/**
 * 参数实体类
 * (1) 帮助，可以显示命令的帮助信息 help h 默认参数
 * (2) 要采集数据的位置 source s
 * (3) 生成待上传的临时目录 pending_dir t "/tmp/pending/sentiment"
 * (4) 生成要上传到的HDFS路径 output o
 */
public class SentimentOptions extends OptionsBase {
    @Option(
            name = "help",
            abbrev = 'h',
            help = "打印帮助信息",
            defaultValue = "true"
    )
    public boolean help;

    @Option(
            name = "source",
            abbrev = 's',
            help = "要采集数据的位置",
            defaultValue = ""
    )
    public String sourceDir;

    @Option(
            name = "pending_dir",
            abbrev = 'p',
            help = "生成待上传的待上传目录",
            defaultValue = "/tmp/pending/sentiment"
    )
    public String pendingDir;

    @Option(
            name = "output",
            abbrev = 'o',
            help = "生成要上传到的HDFS路径",
            defaultValue = ""
    )
    public String output;
}
