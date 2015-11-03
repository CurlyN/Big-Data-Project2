import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Map;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		Hashtable<Integer, ArrayList<Integer>> commentData = new Hashtable<Integer, ArrayList<Integer>>();

		// We know there is only one cache file, so we only retrieve that URI
		File file= new File("/homes/th301/eclipse-projects/Hadoop/Stackoverflow/resources/Posts.xml");
		FileReader fr;
		try {
			fr = new FileReader(file);
			@SuppressWarnings("resource")
			BufferedReader br = new BufferedReader(fr);
			String line;
			long postId=0, postIdType=0, ownerUserId=0, parentId=0;
			while((line=br.readLine())!=null){
				if(line.contains("row")){
					Map<String, String> parsedXML = MRDPUtils.transformXmlToMap(line);
					String postIdS = parsedXML.get("Id");
					String postIdTypeS = parsedXML.get("PostTypeId");
					String ownerUserIdS= parsedXML.get("OwnerUserId");
					String parentIdS=parsedXML.get("ParentId");
					postId = postIdS!=null ? Long.valueOf(postIdS) : 0;
					postIdType = postIdTypeS!=null ? Long.valueOf(postIdTypeS) : 0;
					ownerUserId = ownerUserIdS!=null ? Long.valueOf(ownerUserIdS):0;
					
					//long postId = Long.valueOf(parsedXML.get("Id"));
					//long postIdType = Long.valueOf(parsedXML.get("PostId"));
					//long ownerUserId = Long.valueOf(parsedXML.get("OwnerUserId"));
					//long parentId = 0;
//					ArrayList<Long> commentUsers;
//					commentUsers = commentData.get(new Long(postId));
//					ArrayWritable aw = new ArrayWritable(IntWritable.class);
//					LongWritable[] values = new LongWritable[commentUsers.size()];
//					for (int i = 0; i < commentUsers.size(); i++) {
//						values[i] = new LongWritable(commentUsers.get(i));
//						aw.set(values);
//					}
//					
					if (postIdType == 2) {
						parentId = parentIdS!=null?Long.valueOf(parentIdS):0;
						//parentId = Integer.parseInt(parsedXML.get("ParentId"));
//						postIdOutput.set(parentId);
					} else {
//						postIdOutput.set(postId);
					}
					System.out.println("PostId: "+postId+"\t"+"PostType: "+postIdType+"\t"+"OwnerUserId: "+ownerUserId+"\t"+"ParentId: "+parentId);
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
