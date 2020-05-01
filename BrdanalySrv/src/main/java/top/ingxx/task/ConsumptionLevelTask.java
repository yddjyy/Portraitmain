package top.ingxx.task;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;

import java.util.List;
import top.ingxx.entity.ConsumptionLevel;
import top.ingxx.map.CounsumptionLevelMap;
import top.ingxx.reduce.ConsumptionLeaveFinalReduce;
import top.ingxx.reduce.ConsumptionLevelReduce;
import top.ingxx.util.MongoUtils;

public class ConsumptionLevelTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<ConsumptionLevel> mapresult = text.map(new CounsumptionLevelMap());
        DataSet<ConsumptionLevel> reduceresult = mapresult.groupBy("groupfield").reduceGroup(new ConsumptionLevelReduce());
        DataSet<ConsumptionLevel> reduceresultfinal = reduceresult.groupBy("groupfield").reduce(new ConsumptionLeaveFinalReduce());
        try {
            List<ConsumptionLevel> reusltlist = reduceresultfinal.collect();
            for(ConsumptionLevel consumptionLevel:reusltlist){
                String consumptiontype = consumptionLevel.getConsumptiontype();
                Long count = consumptionLevel.getCount();

                Document doc = MongoUtils.findoneby("consumptionlevelstatics","youfanPortrait",consumptiontype);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",consumptiontype);
                    doc.put("count",count);
                }else{
                    Long countpre = doc.getLong("count");
                    Long total = countpre+count;
                    doc.put("count",total);
                }
                MongoUtils.saveorupdatemongo("consumptionlevelstatics","youfanPortrait",doc);
            }
            env.execute("ConsumptionLevelTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

