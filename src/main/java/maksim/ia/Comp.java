package maksim.ia;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Date;

import scala.Tuple3;

public class Comp implements Comparator<Tuple3<Date, String, String>>, Serializable {
    
    @Override
    public int compare(Tuple3<Date, String, String> o1, Tuple3<Date, String, String> o2) {
        return o1._1().compareTo(o2._1());
    }
}
