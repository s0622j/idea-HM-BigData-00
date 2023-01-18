import com.google.devtools.common.options.OptionsParser;

import java.util.Collections;

/**
 * @description:
 * @author: Allen Woon
 * @time: 2021/1/8 19:18
 */
public class Server {
    public static void main(String[] args) {
        //创建一个参数Parser解析对象  解析规则为ServerOptions中定义
        OptionsParser parser = OptionsParser.newOptionsParser(ServerOptions.class);
        //解析输入参数数组args
        parser.parseAndExitUponError(args);
        //获得Option
        ServerOptions options = parser.getOptions(ServerOptions.class);

        //如果输入参数host为空 或 port<0（非法）或 dirs目录为空
        if (options.host.isEmpty() || options.port < 0 || options.dirs.isEmpty()) {
            //输出Usage使用方法
            printUsage(parser);
            return;
        }
        //输出Server运行的host port
        System.out.format("Starting server at %s:%d...\n", options.host, options.port);
        //输出每个dir的名字
        for (String dirname : options.dirs) {
            System.out.format("\\--> Serving static files at <%s>\n", dirname);
        }
    }

    private static void printUsage(OptionsParser parser) {
        System.out.println("Usage: java -jar server.jar OPTIONS");
        System.out.println(parser.describeOptions(Collections.<String, String>emptyMap(),
                OptionsParser.HelpVerbosity.LONG));
    }
}

