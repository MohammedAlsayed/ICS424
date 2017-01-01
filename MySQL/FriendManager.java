package MySQL; /**
 * Created by alsayed on 12/31/16.
 */
import com.sun.corba.se.impl.orbutil.concurrent.Sync;

import java.sql.*;
import java.util.Random;
import java.util.concurrent.SynchronousQueue;

public class FriendManager {

    public static boolean truncateTable() throws SQLException{
        String sql = "TRUNCATE FRIENDS";
        // Execute deletion

        // creating connection: this form of try is used to close resources after execution
        try (Connection connection = DBUtil.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);){

            int affected = statement.executeUpdate();
            if(affected == 0){
                return true;
            }
            else{
                return false;
            }
        }
    }

    public synchronized static boolean createFriends(int id1, int id2) throws SQLException{
        String sql = "INSERT INTO FRIENDS VALUES (?, ?, ?)";

        // creating connection: this form of try is used to close resources after execution
        try(Connection connection = DBUtil.getConnection();
            PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);){

            statement.setString(1, null);
            statement.setInt(2, id1);
            statement.setInt(3, id2);

            int affected = statement.executeUpdate();

            if(affected == 1){
                return true;
            }
            else{
                return false;
            }
        }
    }



    /**
     * The method will populate 1000 friends in the following way:
     * 100 person, and every person has 10 friends.
     * The 100 person and 10 friends will be chosen from
     * a random number generator
     * @return the number of persons added
     * @throws SQLException
     */
    public static int populate1000rows(){
        int personCounts = 1;
        int friendsCount = 1;
        Random personRandom = new Random();
        Random friendRandom = new Random();
        int numberRows = 0;

        while(personCounts <= 100){
            int personNumber = personRandom.nextInt(Person.PERSON_ROWS);
            while(friendsCount <= 10){
                int friendNumber = friendRandom.nextInt(Person.PERSON_ROWS);
                try {
                    if(personNumber != friendNumber) {
                        createFriends(personNumber, friendNumber);
                        createFriends(friendNumber, personNumber);
                        friendsCount++;
                        numberRows++;
                    }
                }catch (SQLException e){
                    System.out.println(e.getMessage());
                }
            }
            friendsCount = 1;
            personCounts++;
        }
        return numberRows;
    }

}
