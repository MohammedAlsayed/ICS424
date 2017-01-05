package MySQL; /**
 * Created by alsayed on 12/31/16.
 */

import java.sql.*;
import java.util.Random;

public class FriendManager {

    public static boolean truncateTable() throws SQLException {
        String sql = "TRUNCATE FRIENDS";

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
    public synchronized static boolean createRelationships(String DBname, int id1, int id2) throws SQLException {
        String sql = "INSERT INTO" + DBname + "VALUES (?, ?)";

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
     * @return
     */
    public static int seed(String numberOfRelationships) {
        String DBname = "";
        int numberOfFriends = 0;   // number of friends for each person.
        if(numberOfRelationships.equals("100K")) {
            DBname = "realtionships_100K";
            numberOfFriends = 2;
        }

        else if(numberOfRelationships.equals("1M")) {
            DBname = "realtionships_1M";
            numberOfFriends = 20;
        }
        else if(numberOfRelationships.equals("10M")) {
            DBname = "realtionships_10M";
            numberOfFriends = 200;
        }
        else{
            return -1;
        }

        Random randomNum = new Random();
        int insertedFriends = 0;
        int insertedRows = 0;
        int p1 = 1;         // person1
        while (p1 <= 50000) {
            while (insertedFriends < numberOfFriends) {
                int p2 = randomNum.nextInt(50000);  // person2
                try {
                    if (p1 != p2) {
                        createRelationships(DBname, p1, p2);
                        createRelationships(DBname, p2, p1);
                        insertedFriends++;
                        insertedRows++;
                    }
                } catch (SQLException e) {

                }
            }
            insertedFriends = 0;
            p1++;
        }
        return insertedRows;
    }

    public static ResultSet getFOF() throws SQLException{
        String sql =
                "select q1.person1, q8.person2\n" +
                "from\n" +
                "(select * from friends_500_thous_random where person1 = 4999 ) q1\n" +
                "join\n" +
                "(select * from friends_500_thous_random ) q2\n" +
                "on q1.`person2` = q2.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random ) q3\n" +
                "on q2.`person2` = q3.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random ) q4\n" +
                "on q3.`person2` = q4.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random ) q5\n" +
                "on q4.`person2` = q5.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random ) q6\n" +
                "on q5.`person2` = q6.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random ) q7\n" +
                "on q6.`person2` = q7.`person1`\n" +
                "join\n" +
                "(select * from friends_500_thous_random ) q8\n" +
                "on q7.`person2` = q8.`person1`";
        ResultSet rs;

        try(Connection conn = DBUtil.getConnection();
            Statement stmt = conn.createStatement();
        ){
            rs = stmt.executeQuery(sql);
        }
        return rs;
    }
}
