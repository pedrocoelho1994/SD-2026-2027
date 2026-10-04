package tcp01;
import java.io.Serializable;

public class Place implements Serializable {
    private static final long serialVersionUID = 1L;
    private String name;
    private String country;

    public Place(String name, String country) {
        this.name = name; this.country = country;
    }
    public String getName() { return name; }
    public String getCountry() { return country; }
    @Override public String toString() { return name + " (" + country + ")"; }
}