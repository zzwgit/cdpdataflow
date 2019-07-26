package io.infinivision.flink.udfs;

import java.io.Serializable;

public class IdDTO implements Serializable {

    private Long begin = 0L;
    private Long end = 0L;

    public Long getBegin() {
        return begin;
    }

    public Long getBeginAndNext() {
        return begin++;
    }

    public void setBegin(Long begin) {
        this.begin = begin;
    }

    public Long getEnd() {
        return end;
    }

    public void setEnd(Long end) {
        this.end = end;
    }

    public void init(Long begin, Long end) {
        this.begin = begin;
        this.end = end;
    }

    @Override
    public String toString() {
        return "IdDTO{" +
                "begin=" + begin +
                ", end=" + end +
                '}';
    }
}
