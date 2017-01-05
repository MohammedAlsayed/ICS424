package MySQL;

/**
 * Created by alsayed on 12/31/16.
 */

public class Person {
    public static int PERSON_ROWS = 5000;
    private int id;
    private String name;

    public Person(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "name= '" + name + '\'' +
                ", id= " + id ;
    }

}
