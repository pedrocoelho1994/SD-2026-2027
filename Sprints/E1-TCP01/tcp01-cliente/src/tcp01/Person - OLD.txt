package tcp01;
import java.io.Serializable;

public class Person implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private int age;
    private Place place;

    public Person(String name, int age, Place place) {
        this.name = name; this.age = age; this.place = place;
    }
    public String getName() { return name; }
    public int getAge() { return age; }
    public Place getPlace() { return place; }
    @Override public String toString() { return name + ", " + age + " anos, " + place; }
}