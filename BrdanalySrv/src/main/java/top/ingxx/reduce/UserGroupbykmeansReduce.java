package top.ingxx.reduce;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import top.ingxx.entity.UserGroupInfo;
import top.ingxx.kmeans.Cluster;
import top.ingxx.kmeans.KMeansRunbyusergroup;
import top.ingxx.kmeans.Point;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;

public class UserGroupbykmeansReduce implements GroupReduceFunction<UserGroupInfo,ArrayList<Point>> {
    @Override
    public void reduce(Iterable<UserGroupInfo> iterable, Collector<ArrayList<Point>> collector) throws Exception {
        Iterator<UserGroupInfo> iterator = iterable.iterator();
        ArrayList<float[]> dataSet = new ArrayList<float[]>();
        while(iterator.hasNext()){
            UserGroupInfo userGroupInfo = iterator.next();
            float[] f = new float[]{Float.valueOf(userGroupInfo.getUserid()+""),Float.valueOf(userGroupInfo.getAvramout()+""),Float.valueOf(userGroupInfo.getMaxamout()+""),Float.valueOf(userGroupInfo.getDays()),
                    Float.valueOf(userGroupInfo.getBuytype1()),Float.valueOf(userGroupInfo.getBuytype2()),Float.valueOf(userGroupInfo.getBuytype3()),
                    Float.valueOf(userGroupInfo.getBuytime1()),Float.valueOf(userGroupInfo.getBuytime2()),Float.valueOf(userGroupInfo.getBuytime3()),
                    Float.valueOf(userGroupInfo.getBuytime4())};
            dataSet.add(f);
        }
        KMeansRunbyusergroup kMeansRunbyusergroup =new KMeansRunbyusergroup(6, dataSet);

        Set<Cluster> clusterSet = kMeansRunbyusergroup.run();
        ArrayList<Point> arrayList = new ArrayList<Point>();
        for(Cluster cluster:clusterSet){
            arrayList.add(cluster.getCenter());
        }
        collector.collect(arrayList);
    }
}

