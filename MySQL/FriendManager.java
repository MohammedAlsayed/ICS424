package MySQL; /**
 * Created by alsayed on 12/31/16.
 */

import java.sql.*;
import java.util.Random;

public class FriendManager {

    public static boolean truncateTable() throws SQLException {
        String sql = "TRUNCATE FRIENDS";
        // Execute deletion

        // creating connection: this form of try is used to close resources after execution
        try (Connection connection = DBUtil.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {

            int affected = statement.executeUpdate();
            if (affected == 0) {
                return true;
            } else {
                return false;
            }
        }
    }
//
    public synchronized static boolean createFriends(int id1, int id2) throws SQLException {
        String sql = "INSERT INTO friends_500_thous_random_copy VALUES (?, ?)";

        // creating connection: this form of try is used to close resources after execution
        try (Connection connection = DBUtil.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);) {

            statement.setInt(1, id1);
            statement.setInt(2, id2);

            int affected = statement.executeUpdate();

            if (affected == 1) {
                return true;
            } else {
                return false;
            }
        }
    }


    /**
     *
     * @return the number of friend realtionships added
     * @throws SQLException
     */
    public static int populate500000rows() {
        int range = 400000; //range of id's to chose from the person DB
        int personCounts = 1;
        int friendsCount = 1;
        Random personRandom = new Random();
        Random friendRandom = new Random();
        int numberRows = 0;

        while (personCounts <= 5000) {
            while (friendsCount <= 100) {

                // changing person's and his friend's id at each iteration to be more random in insertion.
                int personNumber = personRandom.nextInt(range);    // person1 to add to the friend DB
                int friendNumber = friendRandom.nextInt(range);    // person2 is the friend of person1
                try {
                    if (personNumber != friendNumber) {
                        createFriends(personNumber, friendNumber);
                        friendsCount++;   // adding friends after creation, so when exception is thrown no friend is added.
                        numberRows++;
                    }
                } catch (SQLException e) {
                    // change there id's when an exception is thrown.
                    personNumber = personRandom.nextInt(range);
                    friendNumber = friendRandom.nextInt(range);
                }
            }
            friendsCount = 1;  // reset the number of friends of person. To start adding friends for a new person
            personCounts++;
        }
        return numberRows;
    }


    /**
     * This method add fiends in sequence
     * e.g for person with id = 1, his friends will be persons with the following id (2,3,4,5,.. etc) up to 100 friends.
     * then person with id = 2 friends will be persons with id's (3,4,5,6,... etc) up to 100 friends.
     * @return
     */
    public static int populate5000000rowsSeq() {
        int personCounts = 1;
        int friendsCount = 1;
        Random personRandom = new Random();
        Random friendRandom = new Random();
        int numberRows = 0;

        int personNumber = 1;
        while (personCounts <= 5000) {
            int friendNumber = personNumber + 1;
            while (friendsCount <= 100) {
                try {
                    if (personNumber < friendNumber) {
                        createFriends(personNumber, friendNumber);
                        friendsCount++;
                        numberRows++;
                    }
                } catch (SQLException e) {
                    if (e.getErrorCode() == 1452) {
                        personNumber++;
                        friendNumber++;
                    }
                }
                friendNumber++;
            }
            friendsCount = 1;
            personCounts++;
            personNumber++;
        }
        return numberRows;
    }

    public static ResultSet getFOF() throws SQLException{
        String sql = "Select *\n" +
                "from \n" +
                "( select * from friends_500_thous_random_copy where person1 = 4999) q1\n" +
                "join \n" +
                "(select * from friends_500_thous_random_copy) q2\n" +
                "on q1.`person2` = q2.`person1`\n" +
                "join \n" +
                "(select * from friends_500_thous_random_copy) q3\n" +
                "on q2.`person2` = q3.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random_copy) q4\n" +
                "on q3.`person2` = q4.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random_copy) q5\n" +
                "on q4.`person2` = q5.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random_copy) q6\n" +
                "on q5.`person2` = q6.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random_copy) q7\n" +
                "on q6.`person2` = q7.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random_copy) q8\n" +
                "on q7.`person2` = q8.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random_copy) q9\n" +
                "on q8.`person2` = q9.`person1`";
        ResultSet rs;

        try(Connection conn = DBUtil.getConnection();
            Statement stmt = conn.createStatement();
        ){
            rs = stmt.executeQuery(sql);
        }
        return rs;
    }
}
