package top.ingxx.reduce;

import org.apache.flink.api.common.functions.ReduceFunction;
import top.ingxx.entity.EmailInfo;

public class EmailReduce implements ReduceFunction<EmailInfo> {
    @Override
    public EmailInfo reduce(EmailInfo emaiInfo, EmailInfo t1) throws Exception {
        String emailtype = emaiInfo.getEmailtype();
        Long count1 = emaiInfo.getCount();

        Long count2 = t1.getCount();

        EmailInfo emaiInfofinal = new EmailInfo();
        emaiInfofinal.setEmailtype(emailtype);
        emaiInfofinal.setCount(count1+count2);

        return emaiInfofinal;
    }
}
