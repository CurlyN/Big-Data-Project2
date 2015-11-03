import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StackoverflowMapper extends Mapper<Object, Text, Text, Post> {

	public void map(Object data, Text value, Context context)
			throws IOException, InterruptedException {

		Text key = new Text();
		Map<String, String> parsedXML = MRDPUtils.transformXmlToMap(value
				.toString());
		String posts = parsedXML.get("posts");
		parsedXML = MRDPUtils.transformXmlToMap(posts);
		Iterator<Entry<String, String>> postIterator = parsedXML.entrySet().iterator();
		while (postIterator.hasNext()) {
			String rowData = postIterator.next().toString();
			parsedXML = MRDPUtils.transformXmlToMap(rowData);
			int postId = Integer.parseInt(parsedXML.get("Id"));
			int postIdType = Integer.parseInt(parsedXML.get("PostId"));
			int ownerUserId = Integer.parseInt(parsedXML.get("OwnerUserId"));
			int parentId = 0;
			Post post;
			if (postIdType == 2) {
				parentId = Integer.parseInt(parsedXML.get("ParentId"));
				key.set(String.valueOf(parentId));
			} else {
				key.set(String.valueOf(postId));
			}
			post = new Post(postId, postIdType, ownerUserId, parentId);
			context.write(key, post);
		}

	}
}