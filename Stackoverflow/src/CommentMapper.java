import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CommentMapper extends Mapper<Object, Text, Text, Comment> {

	public void map(Object data, Text value, Context context)
			throws IOException, InterruptedException {

		Text key = new Text();
		Map<String, String> parsedXML = MRDPUtils.transformXmlToMap(value
				.toString());
		String posts = parsedXML.get("comments");
		parsedXML = MRDPUtils.transformXmlToMap(posts);
		Iterator<Entry<String, String>> postIterator = parsedXML.entrySet()
				.iterator();
		while (postIterator.hasNext()) {
			String rowData = postIterator.next().toString();
			parsedXML = MRDPUtils.transformXmlToMap(rowData);
			int postId = Integer.parseInt(parsedXML.get("PostId"));
			int commentUserId = Integer.parseInt(parsedXML.get("UserId"));
			key.set(String.valueOf(postId));
			Comment comment = new Comment(postId, commentUserId);
			context.write(key, comment);
		}

	}
}