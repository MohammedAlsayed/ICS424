package neo4j;

import MySQL.*;

import org.neo4j.driver.v1.*;

public class Main {
	
	public static void main (String [] args){
		
		
		Driver driver = GraphDatabase.driver("bolt://localhost:7687", AuthTokens.basic("neo4j", "qwerty"));
		long t1 = System.currentTimeMillis();
		Session session = driver.session();
		System.out.println((System.currentTimeMillis() - t1) / 1000.0);
		
		
		StatementResult sr = session.run("match (n) return n.name as name");
		
		
		while(sr.hasNext()){
			Record record = sr.next();
			System.out.println(record.get("name").asString());
		}
		
		
		session.close();
		driver.close();
		
	}
}
