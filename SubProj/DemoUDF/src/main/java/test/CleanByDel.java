package test;

import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.util.ArrayList;

/**
 * Created by babyfish on 16-1-14.
 */
public class CleanByDel extends EvalFunc<Tuple>{
    public static final int DEF_SCHEMA_LEN = 19;
    private int schema_len;

    public CleanByDel()
    {
        schema_len = DEF_SCHEMA_LEN;
    }

    public CleanByDel(String slen)
    {
        schema_len = Integer.parseInt(slen);
    }

    public Tuple exec(Tuple t) throws ExecException
    {
        if (t.size() != schema_len)
        {
            ArrayList<String> list = new ArrayList<String>();
            for (int i = 0; i < schema_len; i++)
            {
                list.add("");
            }
            return TupleFactory.getInstance().newTupleNoCopy(list);
        }
        else
        {
            for (int i = 0; i < schema_len; i++)
            {
                t.set(i, ((DataByteArray)t.get(i)).toString());
            }
            return t;
        }
    }
}
