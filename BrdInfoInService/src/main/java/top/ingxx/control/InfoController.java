package top.ingxx.control;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import top.ingxx.entity.ResultMessage;
import top.ingxx.log.AttentionProductLog;
import top.ingxx.log.BuyCartProductLog;
import top.ingxx.log.CollectProductLog;
import top.ingxx.log.ScanProductLog;
import top.ingxx.utils.ReadProperties;

import javax.servlet.http.HttpServletRequest;
import java.util.Date;

@RestController
@RequestMapping("infolog")
public class InfoController {

    private final String attentionProductLogTopic = ReadProperties.getKey("attentionProductLog");
    private final String buyCartProductLogTopic = ReadProperties.getKey("buyCartProductLog");
    private final String collectProductLogTopic = ReadProperties.getKey("collectProductLog");
    private final String scanProductLogTopic = ReadProperties.getKey("scanProductLog");

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @RequestMapping(value = "helloworld",method = RequestMethod.GET)
    public String hellowolrd(HttpServletRequest req){
        String ip =req.getRemoteAddr();
        ResultMessage resultMessage = new ResultMessage();
        resultMessage.setMessage("hello:"+ip);
        resultMessage.setStatus("succes s");
        String result = JSONObject.toJSONString(resultMessage);
        return result;
    }
    /**
     * AttentionProductLog:{productid:productid....}
     BuyCartProductLog:{productid:productid....}
     CollectProductLog:{productid:productid....}
     ScanProductLog:{productid:productid....}
     * @param recevicelog
     * @param req
     * @return
     */
    @RequestMapping(value = "receivelog",method = RequestMethod.POST)
    public String hellowolrd(String recevicelog,HttpServletRequest req){
        if(StringUtils.isBlank(recevicelog)){
            return null;
        }
        String[] rearrays = recevicelog.split(":",2);
        String classname = rearrays[0];
        String data = rearrays[1];
        String resulmesage= "";

        if("AttentionProductLog".equals(classname)){
            AttentionProductLog attentionProductLog = JSONObject.parseObject(data, AttentionProductLog.class);
            resulmesage = JSONObject.toJSONString(attentionProductLog);
           kafkaTemplate.send(attentionProductLogTopic,resulmesage+"##1##"+new Date().getTime());
        }else if("BuyCartProductLog".equals(classname)){
            BuyCartProductLog buyCartProductLog = JSONObject.parseObject(data,BuyCartProductLog.class);
            resulmesage = JSONObject.toJSONString(buyCartProductLog);
            kafkaTemplate.send(buyCartProductLogTopic,resulmesage+"##1##"+new Date().getTime());
        }else if("CollectProductLog".equals(classname)){
            CollectProductLog collectProductLog = JSONObject.parseObject(data,CollectProductLog.class);
            resulmesage = JSONObject.toJSONString(collectProductLog);
            kafkaTemplate.send(collectProductLogTopic,resulmesage+"##1##"+new Date().getTime());
        }else if("ScanProductLog".equals(classname)){
            ScanProductLog scanProductLog = JSONObject.parseObject(data,ScanProductLog.class);
            resulmesage = JSONObject.toJSONString(scanProductLog);
            kafkaTemplate.send(scanProductLogTopic,resulmesage+"##1##"+new Date().getTime());
        }
        ResultMessage resultMessage = new ResultMessage();
        resultMessage.setMessage(resulmesage);
        resultMessage.setStatus("success");
        String result = JSONObject.toJSONString(resultMessage);
        return result;
    }
}
