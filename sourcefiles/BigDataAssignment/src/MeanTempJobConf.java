
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MeanTempJobConf extends Configured implements Tool {

	// Map Class
	static public class MeanTempMapper extends
			Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(LongWritable offset, Text text, Context context)
				throws IOException, InterruptedException {
			
			Text key = new Text();
			Text value = new Text();
			String[] st = text.toString().split(",");
			//ignore the title line
			if (!st[1].equalsIgnoreCase("ID")) {
				//exclude invalid data
				boolean bool = true;
				try{
					 @SuppressWarnings("unused")
				    double d = Double.parseDouble(st[11]);
					d = Double.parseDouble(st[5]);
					}catch(Exception e)
					{
					   bool = false;
					}
				
				if (bool) {
					//mean temperature for world whole year
					key.set("Y");
					value.set(st[11]);
					context.write(key, value);
					//mean temperature for world summer and winter--S:Summer--W:Winter
					if (Double.parseDouble(st[5]) > 0) {
						if (st[9].equals("5") || st[9].equals("6") || st[9].equals("7")) {
							key.set("S");
						} else if (st[9].equals("11") || st[9].equals("12") || st[9].equals("1")) {
							key.set("W");
						}
						context.write(key, value);
					} else if (Double.parseDouble(st[5]) < 0) {
						if (st[9].equals("5") || st[9].equals("6") || st[9].equals("7")) {
							key.set("W");
						} else if (st[9].equals("11") || st[9].equals("12") || st[9].equals("1")) {
							key.set("S");
						}
						context.write(key, value);
					}
					//mean temperature for UK whole year
					if (st[4].equalsIgnoreCase("UK")) {
						key.set("UK");
						context.write(key, value);
					//mean temperature for AU whole year	
					} else if (st[4].equalsIgnoreCase("AU")) {
						key.set("AU");
						context.write(key, value);
					}
				}			
			}
		}
	}


	// Combiner
	static public class MeanTempCombiner extends
			Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> texts, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			// Calculate sum of counts
			for (Text text : texts) {
				String st = text.toString();
				double value = Double.parseDouble(st);
				sum += value;
				count ++;
			}
			context.write(key, new Text(sum + "," + count));
			//
		}
	}
	
	// Reducer
	static public class MeanTempReducer extends
			Reducer<Text, Text, Text, Text> {
		private Text meanTemp = new Text();

		@Override
		protected void reduce(Text key, Iterable<Text> texts, Context context)
				throws IOException, InterruptedException {
			double sum = 0;
			int count = 0;
			// Calculate mean temperature
			for (Text text : texts) {
				String[] st = text.toString().split(",");
				sum += Double.parseDouble(st[0]);
				count += Integer.parseInt(st[1]);
			}
			double mean = sum/count;
			double formatMean = (double)Math.round(mean*10)/10;
			meanTemp.set(Double.toString(formatMean));
			context.write(key, meanTemp);
			//
		}
	}

	public int run(String[] args) throws Exception {
		Configuration configuration = getConf();

		// Initialising Map Reduce Job
		Job job = new Job(configuration, "MeanTemp Calculate");

		// Set Map Reduce main jobconf class
		job.setJarByClass(MeanTempJobConf.class);

		// Set Mapper class
		job.setMapperClass(MeanTempMapper.class);

		// Set Combiner class
		job.setCombinerClass(MeanTempCombiner.class);

		// set Reducer class
		job.setReducerClass(MeanTempReducer.class);

		// set Input Format
		job.setInputFormatClass(TextInputFormat.class);

		// set Output Format
		job.setOutputFormatClass(TextOutputFormat.class);

		// set Output key class
		job.setOutputKeyClass(Text.class);

		// set Output value class
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true) ? 0 : -1;
	}

	public static void main(String[] args) throws Exception {
		System.exit(ToolRunner.run(new MeanTempJobConf(), args));
	}
}
