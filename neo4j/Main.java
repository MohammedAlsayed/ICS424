package neo4j;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;

import MySQL.*;

import org.neo4j.driver.v1.*;

public class Main {
	
	public static void main (String [] args) throws IOException{
		
		/* Port numbers
		 * 
		 * 7687 : 1e5 relationships, 2 per person
		 * 7777 : 1e6 relationships, 20 per person
		 * 7888 : 1e7 relationships, 200 per person
		 */
		
		
		//insertPersonNodes(50000, 7888);
	
        
        insertFriendRelationships(20, 7777);


		
   

        
		
		// search for all person nodes
//        String searchConnectedNodesQuery = "match (n:Man)<-[:helps]-(m) where n.id = 650 return m.id as id";
//        String getAllNodesQuery = "match (n) return n.name as name";
//        String getLvl4FriendsQuery = "match (n:Man {id:8142})-[:helps]->(m:Man) match (m)-[:helps]->(p) match (p)-[:helps]->(o) match (o)-[:helps]->(q) return count(q) as cnt";
//        
//		StatementResult sr = session.run(getLvl4FriendsQuery);
//		
//		
//		while(sr.hasNext()){
//			
//			Record record = sr.next();
//			System.out.println(record.get("cnt").asInt());
//		}
//        
//        
		
	}
	
	
	public static void insertPersonNodes(int numberOfNodes, int portNumber){
	
		// making arrays of names
        String [] boys = new String [1000];
        String [] girls = new String[1000];
        try{
	        Scanner scanB = new Scanner(new File("C:/Users/mosemos/workspace/ics424/src/main/java/MySQL/boys.txt"));
	        Scanner scanG = new Scanner(new File("C:/Users/mosemos/workspace/ics424/src/main/java/MySQL/girls.txt"));
	        System.out.println("reading from the file....");
	        int index = 0  ;
	        while(scanB.hasNext()){
	            String boy = scanB.next();
	            boys[index] = boy;
	            String girl = scanG.next();
	            girls[index] = girl;
	            index++;
	        }
	        scanB.close();
	        scanG.close();
	        
        }
        catch(IOException e){	
        	e.printStackTrace();
        }
        
        System.out.println("Connecting to Neo4j ...");
        
        // initialize database connection
        Driver driver = GraphDatabase.driver("bolt://localhost:" + portNumber, AuthTokens.basic("neo4j", "qwerty"));
		Session session = driver.session();
		
		Random rnd = new Random();
		
		long t1 = System.nanoTime();
		
        // insert nodes
        for(int i = 0; i < numberOfNodes; i++){
        	
        	String name = boys[rnd.nextInt(1000)] + " " + girls[rnd.nextInt(1000)];
        	
        	session.run("create (n:Person {name: \"" + name +  "\", id: " + i + "})");
        	
        }
        
        long t2 = System.nanoTime();
        
        double time = (t2 - t1) / 1000000000.0;
        
        System.out.println("Inserted " + numberOfNodes + " nodes in " + time + " seconds");
        
		session.close();
		driver.close();
		
	}
	
	
	public static void insertFriendRelationships(int numberOfRelationsPerPerson, int portNumber){
	
		System.out.println("Connecting to Neo4j ...");
        
        // initialize database connection
        Driver driver = GraphDatabase.driver("bolt://localhost:" + portNumber, AuthTokens.basic("neo4j", "qwerty"));
		Session session = driver.session();
		
		Random rnd = new Random();
		
		long t1 = System.nanoTime();
		
		
		// for every person, insert numberOfRelationsPerPerson for him with random people from the db, relationships are bidirectional
		for(int i = 0; i < 50000; i++){
						
			int insertedRelations = 0;
			
			while(insertedRelations < numberOfRelationsPerPerson){
				int otherPersonId = rnd.nextInt(50000);
				if(otherPersonId != i){
					
					String insertRelationsQuery = "match (n:Person {id: " + i + " }) match (m:Person {id: " + otherPersonId + "}) create (n)-[:friend]->(m) create (m)-[:friend]->(n)";
					
					session.run(insertRelationsQuery);
					
					insertedRelations++;
				}
			}
		}
		
		
        long t2 = System.nanoTime();
        
        double time = (t2 - t1) / 1000000000.0;
        
        System.out.println("Inserted " + numberOfRelationsPerPerson * 50 + "K relations in " + time + " seconds");
        
		session.close();
		driver.close();
		
		
	}
	
}
