package top.ingxx.reduce;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.bson.Document;
import top.ingxx.entity.UseTypeInfo;
import top.ingxx.util.MongoUtils;

public class UseTypeSink implements SinkFunction<UseTypeInfo> {
    @Override
    public void invoke(UseTypeInfo value, Context context) throws Exception {
        String usetype = value.getUsetype();
        long count = value.getCount();
        Document doc = MongoUtils.findoneby("usetypestatics","brdPortrait",usetype);
        if(doc == null){
            doc = new Document();
            doc.put("info",usetype);
            doc.put("count",count);
        }else{
            Long countpre = doc.getLong("count");
            Long total = countpre+count;
            doc.put("count",total);
        }
        MongoUtils.saveorupdatemongo("usetypestatics","brdPortrait",doc);
    }
}
