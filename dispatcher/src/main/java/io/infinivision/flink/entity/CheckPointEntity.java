package io.infinivision.flink.entity;

import org.apache.commons.math3.util.MathUtils;
import org.apache.flink.streaming.api.CheckpointingMode;

import java.math.BigDecimal;

public class CheckPointEntity {

    private Long intervalTime;
    private Long minPauseBetweenCheckpoints;
    private Long checkpointTimeout;
    private String mode;
    private Boolean enableExternalizedCheckpoint;
    private String stateBackend;
    private String checkpointDataUri;

    public CheckPointEntity() {
    }

    public CheckPointEntity(Long intervalTime, String mode, String stateBackend, String checkpointDataUri) {
        if (null != intervalTime && intervalTime.longValue() > 0) {
            this.intervalTime = intervalTime;
            this.minPauseBetweenCheckpoints = null != intervalTime && intervalTime > 0 ? intervalTime / 10 : 0;
            this.checkpointTimeout = null != intervalTime && intervalTime > 0 ? intervalTime / 10 : 0;
            this.mode = mode;
            this.enableExternalizedCheckpoint = intervalTime > 0;
            this.stateBackend = stateBackend;
            this.checkpointDataUri = checkpointDataUri;
        }
    }

    public Long getIntervalTime() {
        return intervalTime;
    }

    public void setIntervalTime(Long intervalTime) {
        this.intervalTime = intervalTime;
    }

    public Long getMinPauseBetweenCheckpoints() {
        return minPauseBetweenCheckpoints;
    }

    public void setMinPauseBetweenCheckpoints(Long minPauseBetweenCheckpoints) {
        this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
    }

    public Long getCheckpointTimeout() {
        return checkpointTimeout;
    }

    public void setCheckpointTimeout(Long checkpointTimeout) {
        this.checkpointTimeout = checkpointTimeout;
    }

    public CheckpointingMode getCheckpointingMode() {
        return CheckpointingMode.EXACTLY_ONCE.equals(mode) ? CheckpointingMode.EXACTLY_ONCE : CheckpointingMode.AT_LEAST_ONCE;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }

    public Boolean getEnableExternalizedCheckpoint() {
        return enableExternalizedCheckpoint;
    }

    public void setEnableExternalizedCheckpoint(Boolean enableExternalizedCheckpoint) {
        this.enableExternalizedCheckpoint = enableExternalizedCheckpoint;
    }

    public String getStateBackend() {
        return stateBackend;
    }

    public void setStateBackend(String stateBackend) {
        this.stateBackend = stateBackend;
    }

    public String getCheckpointDataUri() {
        return checkpointDataUri;
    }

    public void setCheckpointDataUri(String checkpointDataUri) {
        this.checkpointDataUri = checkpointDataUri;
    }

    @Override
    public String toString() {
        return "CheckPointEntity{" +
                "intervalTime=" + intervalTime +
                ", minPauseBetweenCheckpoints=" + minPauseBetweenCheckpoints +
                ", checkpointTimeout=" + checkpointTimeout +
                ", mode='" + mode + '\'' +
                ", enableExternalizedCheckpoint=" + enableExternalizedCheckpoint +
                ", stateBackend='" + stateBackend + '\'' +
                ", checkpointDataUri='" + checkpointDataUri + '\'' +
                '}';
    }
}
