package top.ingxx.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import top.ingxx.entity.ConsumptionLevel;

public class ConsumptionLeaveFinalReduce implements ReduceFunction<ConsumptionLevel> {
    @Override
    public ConsumptionLevel reduce(ConsumptionLevel consumptionLevel1, ConsumptionLevel consumptionLevel2) throws Exception {
        String consumptiontype = consumptionLevel1.getConsumptiontype();
        Long count1 = consumptionLevel1.getCount();

        Long count2 = consumptionLevel2.getCount();

        ConsumptionLevel consumptionLevel = new ConsumptionLevel();
        consumptionLevel.setConsumptiontype(consumptiontype);
        consumptionLevel.setCount(count1+count2);
        return consumptionLevel;
    }
}
