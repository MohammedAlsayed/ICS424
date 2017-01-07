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
import java.util.concurrent.TimeUnit;

import MySQL.*;

import org.neo4j.driver.v1.*;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.*;

public class Main {
	
	public static void main (String [] args) throws IOException{
		
		/* Port numbers
		 * 
		 * 7687 : 1e5 relationships, 2 per person
		 * 7777 : 1e6 relationships, 20 per person
		 * 7888 : 1e7 relationships, 200 per person
		 */
		
		
		//insertPersonNodes(50000, 7888);
	
        // insertFriendRelationships(20, 7777);

		// getTimeSummary(7687);
		
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
        Driver driver = GraphDatabase.driver("bolt://localhost:" + portNumber, AuthTokens.basic("neo4j", "neo4j"));
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
        Driver driver = GraphDatabase.driver("bolt://localhost:" + portNumber, AuthTokens.basic("neo4j", "neo4j"));
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
	
	
	public static void getTimeSummary(int portNumber){
		
		System.out.println("Connecting to Neo4j ...");
        
        // initialize database connection
        Driver driver = GraphDatabase.driver("bolt://localhost:" + portNumber, AuthTokens.basic("neo4j", "neo4j"));
		Session session = driver.session();
		
		String lvlOneQuery = "match (n:Person {id:16751})-[:friend]->(m:Person) "
				   	       + "return m.id";
		
		String lvlTwoQuery = "match (n:Person {id:16751})-[:friend]->(m:Person) "
						   + "match (m)-[:friend]->(p) where p <> n "
						   + "return p.id";
		
		
		String lvlThreeQuery = "match (n:Person {id:16751})-[:friend]->(m:Person) "
							 + "match (m)-[:friend]->(p) where p <> n "
							 + "match (p)-[:friend]->(o) where o <> m and o <> n "
							 + "return o.id";
						
		
		String lvlFourQuery = "match (n:Person {id:16751})-[:friend]->(m:Person) "
							+ "match (m)-[:friend]->(p)where p <> n "
							+ "match (p)-[:friend]->(o) where o <> m and o <> n "
							+ "match (o)-[:friend]->(r) where r <> p and r <> m and r <> n "
							+ "return r.id";
		
		String lvlFiveQuery = "match (n:Person {id:16751})-[:friend]->(m:Person) "
							+ "match (m)-[:friend]->(p) where p <> n "
							+ "match (p)-[:friend]->(o) where o <> m and o <> n "
							+ "match (o)-[:friend]->(r) where r <> p and r <> m and r <> n "
							+ "match (r)-[:friend]->(b) where b <> o and b <> p and b <> m and b <> n "
							+ "return b.id";
		
		String lvlSixQuery = "match (n:Person {id:16751})-[:friend]->(m:Person) "
						   + "match (m)-[:friend]->(p) where p <> n "
						   + "match (p)-[:friend]->(o) where o <> m and o <> n "
					   	   + "match (o)-[:friend]->(r) where r <> p and r <> m and r <> n "
						   + "match (r)-[:friend]->(b) where b <> o and b <> p and b <> m and b <> n "
						   + "match (b)-[:friend]->(c) where c <> r and c <> o and c <> p and c <> m and c <> n "
						   + "return c.id";
		
		
		long t1 = System.nanoTime();
		

		
		try(Transaction tx = session.beginTransaction()){
			ResultSummary resultSummary = tx.run(lvlOneQuery).consume();
			
			System.out.println("LVL1: Available after: " + resultSummary.resultAvailableAfter(TimeUnit.MILLISECONDS));
			System.out.println("LVL1: Consumed after: " + resultSummary.resultConsumedAfter(TimeUnit.MILLISECONDS));
			
		}
		
		System.out.println();
		
		try(Transaction tx = session.beginTransaction()){
			ResultSummary resultSummary = tx.run(lvlTwoQuery).consume();
			
			System.out.println("LVL2: Available after: " + resultSummary.resultAvailableAfter(TimeUnit.MILLISECONDS));
			System.out.println("LVL2: Consumed after: " + resultSummary.resultConsumedAfter(TimeUnit.MILLISECONDS));		
		}
		
		System.out.println();
		
		try(Transaction tx = session.beginTransaction()){
			ResultSummary resultSummary = tx.run(lvlThreeQuery).consume();
			
			System.out.println("LVL3: Available after: " + resultSummary.resultAvailableAfter(TimeUnit.MILLISECONDS));
			System.out.println("LVL3: Consumed after: " + resultSummary.resultConsumedAfter(TimeUnit.MILLISECONDS));		
		}		
		
		System.out.println();
		
		try(Transaction tx = session.beginTransaction()){
			ResultSummary resultSummary = tx.run(lvlFourQuery).consume();
			
			System.out.println("LVL4: Available after: " + resultSummary.resultAvailableAfter(TimeUnit.MILLISECONDS));
			System.out.println("LVL4: Consumed after: " + resultSummary.resultConsumedAfter(TimeUnit.MILLISECONDS));		
		}		
		
		System.out.println();
		
		try(Transaction tx = session.beginTransaction()){
			ResultSummary resultSummary = tx.run(lvlFiveQuery).consume();
			
			System.out.println("LVL5: Available after: " + resultSummary.resultAvailableAfter(TimeUnit.MILLISECONDS));
			System.out.println("LVL5: Consumed after: " + resultSummary.resultConsumedAfter(TimeUnit.MILLISECONDS));		
		}		
		
		
		
		
		long t2 = System.nanoTime();
        
        double time = (t2 - t1) / 1000000000.0;
        
        System.out.println("Finished summary in " + time + " seconds");
        
		session.close();
		driver.close();	
	}
	
}
