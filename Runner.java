import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by Celaeser on 2017/5/19.
 */
public class Runner {
    public static void main(String args[]) throws Exception{
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "City Sort");

        job.setJarByClass(Runner.class);
        // Set input directory
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        // Set mapper class
        job.setMapperClass(CityMapper.class);
        // Set middle types
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(CityBean.class);
        // Set partition type
        job.setPartitionerClass(CityPartitioner.class);
        job.setNumReduceTasks(2);
        // Set reducer
        job.setReducerClass(CityReducer.class);
        // Set output types
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        // Set output directory
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
