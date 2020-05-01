package top.ingxx.task;

import org.apache.flink.api.java.utils.ParameterTool;
import org.bson.Document;
import top.ingxx.entity.EmailInfo;
import top.ingxx.map.EmailMap;
import top.ingxx.reduce.EmailReduce;
import top.ingxx.util.MongoUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.util.List;
public class EmailTask {
    public static void main(String[] args) {
        args=new String[2];
        args[0]="--input";
        args[1]="hdfs://192.168.73.105:9000/bairuidadb/tb_user/part-m-00000";
        final ParameterTool params = ParameterTool.fromArgs(args);
        System.out.println(params.toMap());
        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));
        try {
            System.out.println(text.collect().size());
        }catch (Exception e){

        }
        DataSet<EmailInfo> mapresult = text.map(new EmailMap());
        System.out.println("mapresult");
        DataSet<EmailInfo> reduceresutl = mapresult.groupBy("groupfield").reduce(new EmailReduce());

        try {
            System.out.println("reduceresult:"+reduceresutl.collect().size());
            List<EmailInfo> reusltlist = reduceresutl.collect();
            for(EmailInfo emaiInfo:reusltlist){
                System.out.println("email="+emaiInfo);
                String emailtype = emaiInfo.getEmailtype();
                Long count = emaiInfo.getCount();

                Document doc = MongoUtils.findoneby("emailstatics","brdPortrait",emailtype);
                if(doc == null){
                    doc = new Document();
                    doc.put("info",emailtype);
                    doc.put("count",count);
                }else{
                    Long countpre = doc.getLong("count");
                    Long total = countpre+count;
                    doc.put("count",total);
                }
                MongoUtils.saveorupdatemongo("emailstatics","brdPortrait",doc);
            }
            env.execute("email analy");
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
