package en.ac.myck;

import com.google.common.collect.Lists;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class MutualFriendCount {
    public static class MutualFriendMapper extends Mapper<Object, Text, Text, Text>{
        private Text _K = new Text();
        private Text _V = new Text();
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String [] s = value.toString().split("->"); // Key Value Pair split
            String [] fl = s[1].split(" "); // firend list
            for(String f : fl){
                if(s[0].compareTo(f)<0) _K.set((s[0]+f).toString());
                else _K.set((f+s[0]).toString());
                _V.set(s[1]);
                context.write(_K,_V);
            }
        }
    }

    public static class MutualFirendReducer extends Reducer<Text, Text, Text, Text>{
        private Text _V = new Text();
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<String>[] fl = (ArrayList<String> []) new ArrayList[2] ;
            int i =0;

            for(Text sfl : values){
                fl[i++] = Lists.newArrayList(sfl.toString().split(" "));
            }
            fl[0].retainAll(fl[1]);
            _V.set(Integer.toString(fl[0].size()));
            context.write(key, _V);
        }
    }
    public static void main(String [] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf,"MutualFriendCounter");
        job.setJarByClass(MutualFriendCount.class);
        job.setMapperClass(MutualFriendMapper.class);
        job.setReducerClass(MutualFirendReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("input/MutualFriend"));

        Path outputPath = new Path("output/MutualFriend");

        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileOutputFormat.setOutputPath(job, outputPath);

        System.exit(job.waitForCompletion(true) ? 0 : 1);


    }
}
