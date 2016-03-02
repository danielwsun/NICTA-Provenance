package test;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

/**
 * Created by babyfish on 16-3-1.
 */
public class CalculateRank extends EvalFunc<Long> {
    public Long exec(Tuple t) throws ExecException
    {
        long a = (Long)t.get(0);
        long b = (Long)t.get(1);
        return a * a + b * b;
    }
}
