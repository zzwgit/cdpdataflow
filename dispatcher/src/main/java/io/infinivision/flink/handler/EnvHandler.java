package io.infinivision.flink.handler;

import io.infinivision.flink.entity.CheckPointEntity;
import io.infinivision.flink.entity.ContextInfoEntity;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.client.gateway.SessionContext;
import org.apache.flink.table.client.gateway.local.ExecutionContext;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class EnvHandler {

    private static EnvHandler INSTANCE = null;

    public static synchronized EnvHandler getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new EnvHandler();
        }
        return INSTANCE;
    }

    public void setAttributes(ContextInfoEntity contextInfo) throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {

        CheckPointEntity checkPointEntity = contextInfo.getCheckPointEntity();
        if (null == checkPointEntity || null == checkPointEntity.getIntervalTime() || checkPointEntity.getIntervalTime() <= 0) {
            return;
        }

        ExecutionContext<?> executionContext =
                contextInfo.getExecutor().getOrCreateExecutionContext(contextInfo.getSessionContext());
//        Method executorMethod = contextInfo.getExecutor().getClass().getDeclaredMethod("getOrCreateExecutionContext",
//                SessionContext.class);
//        executorMethod.setAccessible(true);
//        ExecutionContext<?> executionContext = (ExecutionContext<?>) executorMethod.invoke(contextInfo.getExecutor(), contextInfo.getSessionContext());
        ExecutionContext.EnvironmentInstance envInst = executionContext.createEnvironmentInstance();
        StreamExecutionEnvironment env = envInst.getStreamExecutionEnvironment();


        //是否启动checkpoint
        env.enableCheckpointing(checkPointEntity.getIntervalTime(), checkPointEntity.getCheckpointingMode());

        CheckpointConfig config = env.getCheckpointConfig();

        config.setMinPauseBetweenCheckpoints(checkPointEntity.getMinPauseBetweenCheckpoints());
        config.setCheckpointTimeout(checkPointEntity.getCheckpointTimeout());

        if (checkPointEntity.getEnableExternalizedCheckpoint()) {
            config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

            String stateBackend = checkPointEntity.getStateBackend();
            String checkpointDataUri = checkPointEntity.getCheckpointDataUri();

            //TODO
            switch (stateBackend) {
                case "rocksdb":
                    env.setStateBackend(new RocksDBStateBackend(checkpointDataUri, true));
                    break;
                case "filesystem":
                    env.setStateBackend(new FsStateBackend(checkpointDataUri, true));
//                    env.setStateBackend(new FsStateBackend("hdfs://namenode:40010/flink/checkpoints"));
                    break;
                default:
                    throw new IllegalArgumentException("stateBackend can only be RocksDB or Fs.");
            }
        }
    }
}
