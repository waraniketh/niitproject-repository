import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class Taxtop10 {
	public static class Mymapper1 extends
			Mapper<LongWritable, Text, Text, Text> {
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split(",");
			if (!str[4].equals("Nonfiler")) {
				context.write(new Text("all"), value);
			}
		}
	}

	public static class Myreducer1 extends
			Reducer<Text, Text, Text, DoubleWritable> {
		double[] top10 = new double[10];
		int counter = 0;

		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text value : values) {
				String[] str = value.toString().split(",");
				double dtr = Double.parseDouble(str[5]);
				if (top10[0] < dtr) {
					top10[0] = dtr;
				}
				for (int i = 0; i < top10.length; i++) {
					for (int j = 0; j < top10.length - 1; j++) {
						if (top10[j] > top10[j + 1]) {
							double dir1 = top10[j + 1];
							top10[j + 1] = top10[j];
							top10[j] = dir1;
						}
					}
				}
			}
			double sumofincome=0;
			for (int k = 0; k < top10.length; k++) {
				sumofincome+=top10[k];
			}
			
			double average=sumofincome/top10.length;
			context.write(new Text("average income"), new DoubleWritable(
					average));
		
			
		}
	}

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Taxtop10.class);
		job.setMapperClass(Mymapper1.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setReducerClass(Myreducer1.class);
		FileInputFormat.addInputPath(job, new Path("/dataforjava"));
		FileOutputFormat.setOutputPath(job, new Path(args[0]));
		job.waitForCompletion(true);

	}

}
