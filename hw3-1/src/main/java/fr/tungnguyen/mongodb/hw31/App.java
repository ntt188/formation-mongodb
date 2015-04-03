package fr.tungnguyen.mongodb.hw31;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Sorts.ascending;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class App {
	private static final String _ID = "_id";
	private static final String STUDENT_ID = "student_id";
	
	public static void main(String[] args) {
		MongoClient client = new MongoClient();
		try {
			
			MongoDatabase db = client.getDatabase("students");
	        MongoCollection<Document> collection = db.getCollection("grades");
	        
	        MongoCursor<Document> cursor = collection.find(eq("type", "homework"))
	        		.sort(ascending(STUDENT_ID, "score")).iterator();
	        Double previousStudentId = null;
	        Double currentStudentId;
	        while(cursor.hasNext()) {
	        	Document document = cursor.next();	        	
	        	currentStudentId = document.getDouble(STUDENT_ID);
	        	if (previousStudentId == null || !currentStudentId.equals(previousStudentId)) {
	        		collection.deleteOne(eq(_ID, document.get(_ID)));	        			
	        	}
	        	previousStudentId = currentStudentId;	        		
	        }	        
	        
		} finally {
			client.close();
		}
	}
}
