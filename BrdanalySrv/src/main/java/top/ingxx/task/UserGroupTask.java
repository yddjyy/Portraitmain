package top.ingxx.task;

import org.apache.flink.api.java.utils.ParameterTool;
import top.ingxx.entity.UserGroupInfo;
import top.ingxx.kmeans.Cluster;
import top.ingxx.kmeans.KMeansRunbyusergroup;
import top.ingxx.kmeans.Point;
import top.ingxx.map.KMeansFinalusergroupMap;
import top.ingxx.map.UserGroupMap;
import top.ingxx.map.UserGroupMapbyreduce;
import top.ingxx.reduce.UserGroupInfoReduce;
import top.ingxx.reduce.UserGroupbykmeansReduce;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class UserGroupTask {
    public static void main(String[] args) {
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // get input data
        DataSet<String> text = env.readTextFile(params.get("input"));

        DataSet<UserGroupInfo> mapresult = text.map(new UserGroupMap());
        DataSet<UserGroupInfo> reduceresutl = mapresult.groupBy("groupfield").reduce(new UserGroupInfoReduce());
        DataSet<UserGroupInfo> mapbyreduceresult = reduceresutl.map(new UserGroupMapbyreduce());
        DataSet<ArrayList<Point>> finalresult =  mapbyreduceresult.groupBy("groupfield").reduceGroup(new UserGroupbykmeansReduce());

        try {
            List<ArrayList<Point>> reusltlist = finalresult.collect();
            ArrayList<float[]> dataSet = new ArrayList<float[]>();
            for(ArrayList<Point> array:reusltlist){
                for(Point point:array){
                    dataSet.add(point.getlocalArray());
                }
            }
            KMeansRunbyusergroup kMeansRunbyusergroup =new KMeansRunbyusergroup(6, dataSet);

            Set<Cluster> clusterSet = kMeansRunbyusergroup.run();
            List<Point> finalClutercenter = new ArrayList<Point>();
            int count= 100;
            for(Cluster cluster:clusterSet){
                Point point = cluster.getCenter();
                point.setId(count++);
                finalClutercenter.add(point);
            }
            DataSet<Point> flinalMap = mapbyreduceresult.map(new KMeansFinalusergroupMap(finalClutercenter));
            env.execute("UserGroupTask analy");
        } catch (Exception e) {
            e.printStackTrace();
        }


    }


}

