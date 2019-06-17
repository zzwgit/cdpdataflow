#!/usr/bin/env bash

target="$0"

iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

# Convert relative path to absolute path
bin=`dirname "$target"`

# get flink config
. "$bin"/config.sh

if [ "$FLINK_IDENT_STRING" = "" ]; then
        FLINK_IDENT_STRING="$USER"
fi

CC_CLASSPATH=`constructFlinkClassPath`

log=$FLINK_LOG_DIR/flink-$FLINK_IDENT_STRING-Dispatcher-client-$HOSTNAME.log
log_setting=(-Dlog.file="$log" -Dlog4j.configuration=file:"$FLINK_CONF_DIR"/log4j-cli.properties -Dlogback.configurationFile=file:"$FLINK_CONF_DIR"/logback.xml)

export FLINK_ROOT_DIR
export FLINK_CONF_DIR

# get path of jar in /opt if it exist
FLINK_SQL_CLIENT_JARS=''
for jar in `find "$FLINK_OPT_DIR" -name *.jar`; do
    if [[ -z $FLINK_SQL_CLIENT_JARS ]]; then
        FLINK_SQL_CLIENT_JARS=$jar
    else
        FLINK_SQL_CLIENT_JARS="$FLINK_SQL_CLIENT_JARS":"$jar"
    fi
done

FLINK_CONNECTORS_JARS=''
FLINK_CONNECTORS_JARS_ARGS=''
for jar in `find "$FLINK_OPT_DIR/connectors" -name '*.jar'`; do
    if [[ -z $FLINK_CONNECTORS_JARS ]]; then
        FLINK_CONNECTORS_JARS=$jar
        FLINK_CONNECTORS_JARS_ARGS="--jar $jar"
    else
        FLINK_CONNECTORS_JARS="$FLINK_CONNECTORS_JARS":"$jar"
        FLINK_CONNECTORS_JARS_ARGS="$FLINK_CONNECTORS_JARS_ARGS --jar $jar"
    fi
done

JVM_ARGS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=9999 $JVM_ARGS"

# Add HADOOP_CLASSPATH to allow the usage of Hadoop file systems
exec $JAVA_RUN $JVM_ARGS "${log_setting[@]}" -classpath "`manglePathList "$CC_CLASSPATH:$INTERNAL_HADOOP_CLASSPATHS:$FLINK_CONNECTORS_JARS:$FLINK_SQL_CLIENT_JARS"`" io.infinivision.flink.core.DispatcherHttpServer "$@" $FLINK_CONNECTORS_JARS_ARGS
