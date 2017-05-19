import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Created by Celaeser on 2017/5/19.
 */
public class CityReducer extends Reducer<LongWritable, CityBean, LongWritable, Text> {
    @Override
    protected void reduce(LongWritable key, Iterable<CityBean> values, Context context) throws IOException, InterruptedException {
        Iterator<CityBean> itr = values.iterator();
        ArrayList<CityBean> cities = new ArrayList<CityBean>();

        while(itr.hasNext()){
            cities.add(itr.next().clone());
            //context.write(key, new Text(itr.next().toString()));
        }
        Collections.sort(cities);

        for (CityBean city : cities){
            context.write(key, new Text(city.toString()));
        }
    }
}
