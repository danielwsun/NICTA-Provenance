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
        return (Long)t.get(0) * 1.0 / (Long)t.get(1);
    }
}
