package test;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * Created by babyfish on 16-3-1.
 */
public class ConvertTime extends EvalFunc<Long> {
    public Long exec(Tuple t) throws IOException
    {
        String fa= (String)t.get(1);
        String la= (String)t.get(2);
        long result = convert(la) - convert(fa);
        return (0 == result)?1:result;
    }

    private long convert(String s)
    {
        String[] ss = s.split(":");
        return Long.valueOf(ss[0]) * 60 + Long.valueOf(ss[1].split("[.]")[0]);
    }
}
