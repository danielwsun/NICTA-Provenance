package test;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Created by babyfish on 16-3-2.
 */
public class CalculateDensity extends EvalFunc<Double> {
    public Double exec(Tuple t) throws ExecException
    {
        System.out.println("======================" + Integer.toString(t.size()));
        if (null == t.get(0)) System.out.println("NULL000000000000000");
        if (null == t.get(1)) System.out.println("NULL111111111111111");
        return (Long)t.get(0) * 1.0 / (Long)t.get(1);
    }
}
