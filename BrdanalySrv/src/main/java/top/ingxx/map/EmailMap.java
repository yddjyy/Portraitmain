package top.ingxx.map;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import top.ingxx.entity.EmailInfo;
import top.ingxx.util.EmailUtils;
import top.ingxx.util.HbaseUtils;

import javax.xml.bind.SchemaOutputResolver;

public class EmailMap implements MapFunction<String, EmailInfo> {
    @Override
    public EmailInfo map(String s) throws Exception {
        System.out.println("s1:"+s);
        if(StringUtils.isBlank(s)){
            return null;
        }
        System.out.println("s2:"+s);
        String[] userinfos = s.split(",");
        String userid = userinfos[0];
        String username = userinfos[1];
        String sex = userinfos[2];
        String telphone = userinfos[3];
        String email = userinfos[4];
        String age = userinfos[5];
        String registerTime = userinfos[6];
        String usetype = userinfos[7];//'终端类型：0、pc端；1、移动端；2、小程序端'

        String emailtype = EmailUtils.getEmailtypeBy(email);

        String tablename = "userflaginfo";
        String rowkey = userid;
        String famliyname = "baseinfo";
        String colum = "emailinfo";//运营商
        System.out.println("run hbaseutils");
        HbaseUtils.putdata(tablename,rowkey,famliyname,colum,emailtype);
        EmailInfo emailInfo = new EmailInfo();
        String groupfield = "emailInfo=="+emailtype;
        emailInfo.setEmailtype(emailtype);
        emailInfo.setCount(1l);
        emailInfo.setGroupfield(groupfield);
        return emailInfo;
    }
}
