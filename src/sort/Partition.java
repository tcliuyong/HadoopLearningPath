package sort;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by tcliuyong on 16-5-3.
 */
//返回的是partiton ID
public class Partition extends Partitioner<IntWritable, IntWritable>{

    @Override
    public int getPartition(IntWritable key, IntWritable value, int numPartitions) {
        int maxNum = 65535;
        int bound = maxNum / numPartitions;
        int keynumber = key.get();
        for(int i = 0; i < numPartitions; i++){
            if(keynumber < bound *i && keynumber >= bound*(i - 1)){
                return i - 1;
            }
        }
        return -1;
    }
}
