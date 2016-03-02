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
public class CleanByRep extends EvalFunc<Tuple> {
    public static final int DEF_SCHEMA_LEN = 19;
    private int schema_len;

    public CleanByRep()
    {
        schema_len = DEF_SCHEMA_LEN;
    }

    public CleanByRep(String slen)
    {
        schema_len = Integer.parseInt(slen);
    }

    public Tuple exec(Tuple t) throws ExecException
    {
        //System.out.println("<><>SIZE::" + Integer.toString(t.size()));
        if (t.size() != schema_len)
        {
            ArrayList<String> list = new ArrayList<String>();
            int lim = t.size();
            boolean waiting = false;
            String acc = "";
            for (int i = 0; i < lim; i++)
            {
                String tmp = (null == t.get(i))?"":((DataByteArray)t.get(i)).toString();
                if (waiting)
                {
                    if (0 != tmp.length())
                        if ('\"' == tmp.charAt(tmp.length() - 1))
                        {
                            acc += tmp.substring(0, tmp.length() - 1);
                            list.add(acc);
                            acc = "";
                            waiting = false;
                        }
                        else
                        {
                            acc += tmp;
                        }
                }
                else
                {
                    if (0 == tmp.length())
                    {
                        list.add("");
                    }
                    else if ('\"' == tmp.charAt(0))
                    {
                        waiting = true;
                        acc += tmp.substring(1);
                    }
                    else
                    {
                        list.add(tmp);
                    }
                }
            }
            if (list.size() != schema_len)
            {
                ArrayList<String> lis = new ArrayList<String>();
                for (int i = 0; i < schema_len; i++)
                {
                    lis.add("");
                }
                return TupleFactory.getInstance().newTupleNoCopy(lis);
            }
            return TupleFactory.getInstance().newTupleNoCopy(list);
        }
        else
        {
            for (int i = 0; i < schema_len; i++)
            {
                //System.out.println("ITER::" + Integer.toString(i));
                t.set(i, (null == t.get(i))?"":((DataByteArray)t.get(i)).toString());
            }
            return t;
        }
    }
}
