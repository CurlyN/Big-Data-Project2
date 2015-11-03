import java.io.IOException;
import java.util.Arrays;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StackoverflowApplication {
	
	static enum Counters {
		BadData
	};

	public static void main(String[] args) throws IOException,
			ClassNotFoundException, InterruptedException {
		// Program Execution Starts Here
		FileSystem fs = FileSystem.get(new Configuration());
		fs.delete(new Path(args[args.length - 1]), true);
		String[] inputFilesArgs = Arrays.copyOfRange(args, 0, args.length - 1);
		String outputFileArgs = args[args.length - 1];

		System.out.println("USER: BEFORE RUNNING MAP");
		joinPostsComments(inputFilesArgs, outputFileArgs);
		// runPostsJob(inputFilesArgs, outputFileArgs);
		// runCommentsJob(inputFilesArgs, outputFileArgs);
	}

	private static void joinPostsComments(String[] inputFilesArgs,
			String outputFileArgs) throws IOException, ClassNotFoundException,
			InterruptedException {
		// Job configuration for JOIN
		Job job = Job.getInstance(new Configuration());
		Configuration conf = job.getConfiguration();

		job.setJarByClass(StackoverflowApplication.class);
		job.setMapperClass(JoinMapper.class);

		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(PostCommentPair.class);

		job.addCacheFile(new Path("cache/Comments.xml").toUri());

		Path outputPath = new Path(outputFileArgs);
		FileInputFormat.setInputPaths(job,
				StringUtils.join(inputFilesArgs, ","));
		FileOutputFormat.setOutputPath(job, outputPath);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		job.waitForCompletion(true);

	}

//	public static void runPostsJob(String[] input, String output)
//			throws IOException, ClassNotFoundException, InterruptedException {
//		// Sets up the MR Job and then executes
//
//		Configuration conf = new Configuration();
//		@SuppressWarnings("deprecation")
//		Job job = new Job(conf);
//
//		job.setJarByClass(StackoverflowApplication.class);
//		job.setMapperClass(StackoverflowMapper.class);
//		// job.setReducerClass(StackoverflowReducer.class);
//
//		job.setNumReduceTasks(3);
//
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Post.class);
//
//		Path outputPath = new Path(output);
//
//		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
//		FileOutputFormat.setOutputPath(job, outputPath);
//		outputPath.getFileSystem(conf).delete(outputPath, true);
//
//		job.waitForCompletion(true);
//
//	}
//
//	public static void runCommentsJob(String[] input, String output)
//			throws IOException, ClassNotFoundException, InterruptedException {
//		// Sets up the MR Job and then executes
//
//		Configuration conf = new Configuration();
//		@SuppressWarnings("deprecation")
//		Job job = new Job(conf);
//
//		job.setJarByClass(StackoverflowApplication.class);
//		job.setMapperClass(CommentMapper.class);
//		// job.setReducerClass(StackoverflowReducer.class);
//
//		job.setNumReduceTasks(3);
//
//		job.setMapOutputKeyClass(Text.class);
//		job.setMapOutputValueClass(Comment.class);
//
//		Path outputPath = new Path(output);
//
//		FileInputFormat.setInputPaths(job, StringUtils.join(input, ","));
//		FileOutputFormat.setOutputPath(job, outputPath);
//		outputPath.getFileSystem(conf).delete(outputPath, true);
//
//		job.waitForCompletion(true);
//
//	}

}
