
import java.io.IOException;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Taxfilermine {


	public static class Mymapper1 extends
			Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] columns = value.toString().split(",");
			if (!columns[4].equals("Nonfiler")&&!columns[2].equals("Divorced")){
				context.write(new Text(columns[7]),new Text("count"));
			}
		}
	}
	public static class Myreducer1 extends
			Reducer<Text, Text, Text, IntWritable> {
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (Text retreive : values) {
				count++;
			}
			context.write(key, new IntWritable(count));
		}

	}
	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Taxfilermine.class);
		job.setMapperClass(Mymapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Myreducer1.class);
		FileInputFormat.addInputPath(job, new Path("/dataforjava"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);
	}
}