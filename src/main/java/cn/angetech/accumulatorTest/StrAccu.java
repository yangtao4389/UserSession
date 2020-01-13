package cn.angetech.accumulatorTest;

import org.apache.spark.util.AccumulatorV2;

public class StrAccu extends AccumulatorV2<String,String> {
    private String str = "";



    @Override
    public boolean isZero() {
        return str == "";
    }

    @Override
    public AccumulatorV2<String, String> copy() {
        StrAccu newAccumulator = new StrAccu();
        newAccumulator.str = this.str;
        return newAccumulator;
    }

    @Override
    public void reset() {
        str = "";
    }

    @Override
    public void add(String v) {
        str += v+"_";
    }
    @Override
    public void merge(AccumulatorV2<String, String> other) {
        StrAccu o = (StrAccu)other;
        str += o.str;
    }
    @Override
    public String value() {
        return str;
    }
}
