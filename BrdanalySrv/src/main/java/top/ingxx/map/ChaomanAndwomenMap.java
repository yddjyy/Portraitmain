package top.ingxx.map;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import top.ingxx.entity.ChaomanAndWomenInfo;
import top.ingxx.kafka.KafkaEvent;
import top.ingxx.log.ScanProductLog;
import top.ingxx.utils.ReadProperties;

import java.util.ArrayList;
import java.util.List;

public class ChaomanAndwomenMap implements FlatMapFunction<KafkaEvent, ChaomanAndWomenInfo> {

    @Override
    public void flatMap(KafkaEvent kafkaEvent, Collector<ChaomanAndWomenInfo> collector) throws Exception {
        String data = kafkaEvent.getWord();
        ScanProductLog scanProductLog = JSONObject.parseObject(data,ScanProductLog.class);
        int userid = scanProductLog.getUserid();
        int productid = scanProductLog.getProductid();
        ChaomanAndWomenInfo chaomanAndWomenInfo = new ChaomanAndWomenInfo();
        chaomanAndWomenInfo.setUserid(userid+"");
        String chaotype = ReadProperties.getKey(productid+"","productChaoLiudic.properties");
        if(StringUtils.isNotBlank(chaotype)){
            chaomanAndWomenInfo.setChaotype(chaotype);
            chaomanAndWomenInfo.setCount(1l);
            chaomanAndWomenInfo.setGroupbyfield("chaomanAndWomen=="+userid);
            List<ChaomanAndWomenInfo> list = new ArrayList<ChaomanAndWomenInfo>();
            list.add(chaomanAndWomenInfo);
            collector.collect(chaomanAndWomenInfo);
        }

    }

}

