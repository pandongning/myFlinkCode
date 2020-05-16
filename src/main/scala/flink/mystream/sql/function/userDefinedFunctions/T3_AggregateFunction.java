package flink.mystream.sql.function.userDefinedFunctions;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @author pdn
 */


public class T3_AggregateFunction extends AggregateFunction<Long, WeightedAvgAccum> {

    @Override
    public WeightedAvgAccum createAccumulator() {
        return new WeightedAvgAccum();
    }

    @Override
    public Long getValue(WeightedAvgAccum accumulator) {
        if (accumulator.count == 0) {
            return null;
        } else {
            return accumulator.sum / accumulator.count;
        }
    }

    public void accumulate(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum += iValue * iWeight;
        acc.count += iWeight;
    }

    public void retract(WeightedAvgAccum acc, long iValue, int iWeight) {
        acc.sum -= iValue * iWeight;
        acc.count -= iWeight;
    }



}

