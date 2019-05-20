package io.infinivision.flink.parser;

import org.apache.commons.cli.*;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.client.SqlClientException;
import org.apache.flink.table.client.cli.CliOptionsParser;

import java.io.File;
import java.net.URL;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class OptionsParser {

    public static final Option OPTION_SQLPATH = Option
            .builder("sqlPath")
            .required(true)
            .longOpt("sqlPath")
            .numberOfArgs(1)
            .argName("sql path")
            .desc(
                    "sql path")
            .build();

    public static final Option OPTION_CONF_PATH = Option
            .builder("confPath")
            .required(false)
            .longOpt("confPath")
            .numberOfArgs(1)
            .argName("conf path")
            .desc(
                    "conf path")
            .build();

    public static final Option OPTION_CP_INTERVALTIME = Option
            .builder("intervalTime")
            .required(false)
            .longOpt("intervalTime")
            .numberOfArgs(1)
            .argName("checkpoint interval time")
            .desc(
                    "checkpoint interval time")
            .build();

    public static final Option OPTION_CP_MODE = Option
            .builder("mode")
            .required(false)
            .longOpt("mode")
            .numberOfArgs(1)
            .argName("checkpoint mode")
            .desc(
                    "EXACTLY_ONCE or AT_LEAST_ONCE")
            .build();

    public static final Option OPTION_CP_STATEBACKEND = Option
            .builder("stateBackend")
            .required(false)
            .longOpt("stateBackend")
            .numberOfArgs(1)
            .argName("checkpoint state.backend")
            .desc(
                    "filesystem or rocksdb")
            .build();

    public static final Option OPTION_CP_STATECHECKPOINTSDIR = Option
            .builder("stateCheckpointsDir")
            .required(false)
            .longOpt("stateCheckpointsDir")
            .numberOfArgs(1)
            .argName("checkpoint url")
            .desc(
                    "")
            .build();

    public static final Option OPTION_FROMSAVEPOINT = Option
            .builder("fromSavepoint")
            .required(false)
            .longOpt("fromSavepoint")
            .numberOfArgs(1)
            .argName("restore from savepoint")
            .desc(
                    "restore from savepoint")
            .build();

    public static CommandLine argsToCommandLine(String[] args) throws ParseException {
        Options options = new Options();

        options.addOption(CliOptionsParser.OPTION_SESSION);
        options.addOption(CliOptionsParser.OPTION_DEFAULTS);
        options.addOption(CliOptionsParser.OPTION_ENVIRONMENT);
        options.addOption(CliOptionsParser.OPTION_JAR);
        options.addOption(CliOptionsParser.OPTION_LIBRARY);
        //sqlPath
        options.addOption(OptionsParser.OPTION_SQLPATH);
        //confPath
        options.addOption(OptionsParser.OPTION_CONF_PATH);
        //checkPoint{intervalTime mode stateBackend checkpointDataUri}
        options.addOption(OptionsParser.OPTION_CP_INTERVALTIME);
        options.addOption(OptionsParser.OPTION_CP_MODE);
        options.addOption(OptionsParser.OPTION_CP_STATEBACKEND);
        options.addOption(OptionsParser.OPTION_CP_STATECHECKPOINTSDIR);
        //fromSavepoint
        options.addOption(OptionsParser.OPTION_FROMSAVEPOINT);

        CommandLineParser parser = new DefaultParser();
        return parser.parse(options, args, false);
    }

    public static URL checkUrl(CommandLine line, Option option) {
        final List<URL> urls = checkUrls(line, option);
        if (urls != null && !urls.isEmpty()) {
            return urls.get(0);
        }
        return null;
    }

    public static List<URL> checkUrls(CommandLine line, Option option) {
        if (line.hasOption(option.getOpt())) {
            final String[] urls = line.getOptionValues(option.getOpt());
            return Arrays.stream(urls)
                    .distinct()
                    .map((url) -> {
                        try {
                            return Path.fromLocalFile(new File(url).getAbsoluteFile()).toUri().toURL();
                        } catch (Exception e) {
                            throw new SqlClientException("Invalid path for option '" + option.getLongOpt() + "': " + url, e);
                        }
                    })
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

}
