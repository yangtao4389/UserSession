package cn.angetech.session;

import com.google.common.collect.Lists;
import org.apache.spark.SparkContext;
import org.apache.spark.scheduler.AccumulableInfo;
import org.apache.spark.util.AccumulatorMetadata;
import org.apache.spark.util.AccumulatorV2;
import scala.Option;

import java.util.ArrayList;
import java.util.List;

// T 指泛型
public class CollectionAccumulator<T> extends AccumulatorV2<T,List<T>> {
    private List<T> list = Lists.newArrayList();

    @Override
    public boolean isZero() {
        return list.isEmpty();
    }

    @Override
    public AccumulatorV2<T, List<T>> copy() {
        CollectionAccumulator<T> accumulator = new CollectionAccumulator<>();
        synchronized (accumulator){
            accumulator.list.addAll(list);
        }
        return accumulator;
    }

    @Override
    public void reset() {
        list.clear();
    }

    @Override
    public void add(T v) {
        list.add(v);
    }

    @Override
    public void merge(AccumulatorV2<T, List<T>> other) {
        if(other instanceof CollectionAccumulator){
            list.addAll(((CollectionAccumulator)other).list);
        }else {
            throw new UnsupportedOperationException("cannot merge "+ this.getClass().getName() + "with" + other.getClass().getName());
        }
    }

    @Override
    public List<T> value() {
        return new ArrayList<>(list);
    }
}
