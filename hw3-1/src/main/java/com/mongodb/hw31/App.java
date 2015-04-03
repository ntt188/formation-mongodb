package com.mongodb.hw31;

import static com.mongodb.client.model.Filters.eq;

import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class App {

	private static final String SCORES = "scores";
	private static final String _ID = "_id";

	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		try {
			
			MongoDatabase db = client.getDatabase("school");
	        MongoCollection<Document> collection = db.getCollection("students");
	        
	        MongoCursor<Document> cursor = collection.find().iterator();
	        
	        while(cursor.hasNext()) {
	        	Document documentStudent = cursor.next();
	        	@SuppressWarnings("unchecked")
				List<Document> documentsScore = (List<Document>) documentStudent.get(SCORES);	        	
	        	Document documentMinHomeworkScore = findMinHomeworkScore(documentsScore);	        	
	        	if (documentMinHomeworkScore != null) {	        		
	        		documentsScore.remove(documentMinHomeworkScore);
	        		collection.updateOne(eq(_ID, documentStudent.get(_ID)), 
	        				new Document("$set", new Document(SCORES, documentsScore)));
	        	}	        		        	
	        }        
	        
		} finally {
			client.close();
		}
	}

	private static Document findMinHomeworkScore(List<Document> documentsScore) {
		Double minScoreHomework = 101.0;
		Document documentMinScoreHomework = null;
		for (Document documentScore : documentsScore) {	        		
			if ("homework".equals(documentScore.getString("type"))) {
				Double score = documentScore.getDouble("score");	
				if (score < minScoreHomework) {
					minScoreHomework = score;
					documentMinScoreHomework = documentScore;
				}
			}
		}
		return documentMinScoreHomework;
	}
}
