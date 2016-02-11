package test;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Created by babyfish on 16-2-9.
 */
public class Marker extends EvalFunc<String> {

    //public Marker(){}

    public String exec(Tuple t) throws ExecException
    {
        System.out.println("Marker : " + (String)(t.get(0)));
        return (String)(t.get(0));
    }
}
