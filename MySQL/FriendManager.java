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

    public synchronized static boolean createFriends(int id1, int id2) throws SQLException {
        String sql = "INSERT INTO friends_one_million VALUES (?, ?)";

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
     * The method will populate 1000 friends in the following way:
     * 100 person, and every person has 10 friends.
     * The 100 person and 10 friends will be chosen from
     * a random number generator
     *
     * @return the number of persons added
     * @throws SQLException
     */
    public static int populate500000rows() {
        int personCounts = 1;
        int friendsCount = 1;
        Random personRandom = new Random();
        Random friendRandom = new Random();
        int numberRows = 0;

        while (personCounts <= 5000) {
            int personNumber = personRandom.nextInt(Person.PERSON_ROWS);
            while (friendsCount <= 100) {
                int friendNumber = friendRandom.nextInt(Person.PERSON_ROWS);
                try {
                    if (personNumber != friendNumber) {
                        createFriends(personNumber, friendNumber);
                        friendsCount++;
                        numberRows++;
                    }
                } catch (SQLException e) {
                    personNumber = personRandom.nextInt(Person.PERSON_ROWS);
                    friendNumber = friendRandom.nextInt(Person.PERSON_ROWS);
                }
            }
            friendsCount = 1;
            personCounts++;
        }
        return numberRows;
    }


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

    public static ResultSet getFOF3() throws SQLException{
        String sql =
                "Select *\n" +
                        "from \n" +
                        "( select * from friends_500_thousand where person1 = 4305) q1\n" +
                        "join \n" +
                        "(select * from friends_500_thousand) q2\n" +
                        "on q1.`person2` = q2.`person1`\n" +
                        "join \n" +
                        "(select * from friends_500_thousand) q3\n" +
                        "on q2.`person2` = q3.`person1`\n" ;
        ResultSet rs;

        try(Connection conn = DBUtil.getConnection();
            Statement stmt = conn.createStatement();
        ){
            rs = stmt.executeQuery(sql);
        }
        return rs;
    }
}
