package MySQL;

import java.io.File;
import java.io.FileNotFoundException;
import java.sql.SQLException;
import java.util.Random;
import java.util.Scanner;

/**
 * Created by alsayed on 12/31/16.
 */
public class Main {
    public static void main(String []args) throws Exception{
        // Seeding 100K records
        System.out.println("Started populating 1 !!!");
        long startTime = System.nanoTime();
        FriendManager.seed("100K");
        long estimatedTime = System.nanoTime() - startTime;
        System.out.println("Estimated time 1 : "+estimatedTime/1000000000.0);

        // Seeding 1M records
        System.out.println("Started populating 2 !!!");
        startTime = System.nanoTime();
        FriendManager.seed("1M");
        estimatedTime = System.nanoTime() - startTime;
        System.out.println("Estimated time 2 : "+estimatedTime/1000000000.0);

        // Seeding 10M records
        System.out.println("Started populating 3 !!!");
        startTime = System.nanoTime();
        FriendManager.seed("10M");
        estimatedTime = System.nanoTime() - startTime;
        System.out.println("Estimated time 3 : "+estimatedTime/1000000000.0);
    }




    public static void readAndPopulate() throws FileNotFoundException{
        String [] boys = new String [1000];
        String [] girls = new String[1000];
        Scanner scanB = new Scanner(new File("/Users/alsayed/ICS424/src/MySQL/boys.txt"));
        Scanner scanG = new Scanner(new File("/Users/alsayed/ICS424/src/MySQL/girls.txt"));
        int index = 0  ;
        System.out.println("Start reading from the file....");
        long startRead = System.nanoTime();
        while(scanB.hasNext()){
            String boy = scanB.next();
            boys[index] = boy;
            String girl = scanG.next();
            girls[index] = girl;
            index++;
        }
        scanB.close();
        scanG.close();

        long estimatedRead = System.nanoTime() - startRead;
        System.out.println("finished reading in: "+ estimatedRead/1000000000.0);
        Random f = new Random();
        Random l = new Random();
        int count = 0;
        System.out.println("Start populating the DB");
        long startPopulate = System.nanoTime();
        while(count <= 500000){
            String first = girls[f.nextInt(1000)];
            String second = boys[l.nextInt(1000)];
            try{
                PersonManager.createPerson(first +" " + second);
                count++;
            }catch (SQLException e){

            }
            if(count == 1000){
                System.out.println(count);
            }
            if(count == 10000){
                System.out.println(count);
            }
            if(count == 100000){
                System.out.println(count);
            }
            if(count == 500000){
                System.out.println(count);
            }
        }
        long estimatedPopulate = System.nanoTime() - startPopulate;
        System.out.println("finished populating " + count +" record in: "+ estimatedPopulate/1000000000.0);

    }
}
