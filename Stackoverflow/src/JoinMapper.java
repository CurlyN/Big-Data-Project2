import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JoinMapper extends
		Mapper<Object, Text, IntWritable, PostCommentPair> {
	private Hashtable<Integer, ArrayList<Integer>> commentData;

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {

		commentData = new Hashtable<Integer, ArrayList<Integer>>();

		URI fileUri = context.getCacheFiles()[0];

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataInputStream in = fs.open(new Path(fileUri));

		BufferedReader br = new BufferedReader(new InputStreamReader(in));
		String line;
		Integer postId = 0, commentUserId = 0;
		while ((line = br.readLine()) != null) {
			if (line.contains("row")) {
				Map<String, String> parsedXML = MRDPUtils
						.transformXmlToMap(line);
				String postIdS = parsedXML.get("PostId");
				String commentUserIdS = parsedXML.get("UserId");
				postId = postIdS!=null?Integer.valueOf(postIdS):0;
				commentUserId = commentUserIdS!=null?Integer.valueOf(commentUserIdS):0;
				ArrayList<Integer> commentUsers = commentData
						.get(new Integer(postId));
				commentUsers = commentUsers==null?new ArrayList<Integer>():commentUsers;
				commentUsers.add(commentUserId);
				commentData.put(postId, commentUsers);

			}
		}
		br.close();


		super.setup(context);
		System.out.println("-- JoinMapper.setup finished --");
	}

	public void map(Object data, Text value, Context context)
			throws IOException, InterruptedException {

		IntWritable postIdOutput = new IntWritable();
		PostCommentPair joinedData;

		if (value.toString().contains("row")) {
			Map<String, String> parsedXML = MRDPUtils.transformXmlToMap(value
					.toString());
			Integer postId = 0, postIdType = 0, ownerUserId = 0, parentId = 0;
			String postIdS = parsedXML.get("Id");
			String postIdTypeS = parsedXML.get("PostTypeId");
			String ownerUserIdS = parsedXML.get("OwnerUserId");
			String parentIdS = parsedXML.get("ParentId");
			postId = postIdS != null ? Integer.valueOf(postIdS) : 0;
			postIdType = postIdTypeS != null ? Integer.valueOf(postIdTypeS) : 0;
			ownerUserId = ownerUserIdS != null ? Integer.valueOf(ownerUserIdS) : 0;
			ArrayList<Integer> commentUsers;
			commentUsers = commentData.get(new Integer(postId));
			ArrayList<Integer> aw = new ArrayList<Integer>();
			aw.add(new Integer(0));
			if (commentUsers != null) {
				aw = commentUsers;
			}
			if (postIdType == 2) {
				parentId = parentIdS != null ? Integer.valueOf(parentIdS) : 0;
				postIdOutput.set(parentId);
			} else {
				postIdOutput.set(postId);
			}
			joinedData = new PostCommentPair(new IntWritable(postId),
					new IntWritable(ownerUserId),
					new IntWritable(postIdType), new IntWritable(parentId),
					aw);
			context.write(postIdOutput, joinedData);

		}
	}
}
