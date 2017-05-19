import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Celaeser on 2017/5/19.
 */
public class CityMapper extends Mapper<LongWritable ,Text, LongWritable, CityBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        StringTokenizer cityData = new StringTokenizer(value.toString());
        CityBean city = new CityBean(Long.parseLong(cityData.nextToken()), cityData.nextToken(),
                Long.parseLong(cityData.nextToken()), Long.parseLong(cityData.nextToken()));

        context.write(new LongWritable(city.getProvienceId()), city);
    }
}
