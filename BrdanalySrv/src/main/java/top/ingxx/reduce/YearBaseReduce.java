package top.ingxx.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import top.ingxx.entity.YearBase;

public class YearBaseReduce  implements ReduceFunction<YearBase> {
    @Override
    public YearBase reduce(YearBase yearBase, YearBase t1) throws Exception {
        String yeartype = yearBase.getYeartype();
        Long count1 = yearBase.getCount();

        Long count2 = t1.getCount();

        YearBase finalyearBase = new YearBase();
        finalyearBase.setYeartype(yeartype);
        finalyearBase.setCount(count1+count2);
        return finalyearBase;
    }
}
