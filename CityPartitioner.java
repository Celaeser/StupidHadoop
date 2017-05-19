import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by mac on 2017/5/19
 */
public class CityPartitioner extends Partitioner<LongWritable, CityBean> {
    @Override
    public int getPartition(LongWritable id, CityBean cityBean, int i) {
        return (int) (cityBean.getProvienceId() % 2);
    }
}