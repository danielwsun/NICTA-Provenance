package com.nicta.provenance.pigudf;

import com.google.gson.Gson;
import com.nicta.provenance.ProvConfig;
import com.nicta.provenance.pipeline.LogLine;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Trams Wang
 * @version 1.1
 * Date: Jan. 20, 2016
 *
 *   Provenance storing function. Syntax for using this function in Pig Latin goes below:
 *   STORE varname INTO 'host/port' USING com.nicta.provenance.pigudf.ProvStorer('varname', 'srcidx');
 *   Where 'host/port' is '/' separated host and port of pipeline server; 'varname' is the variable name in Pig Latin
 * that refers to data going to be stored; 'srcidx' is index of data referred by 'varname'.
 */
public class ProvStorer extends StoreFunc{

    /**
     * @author Trams Wang
     * @version 1.0
     * Date: Jan. 20, 2016
     *
     *   Supporting class used by ProvStorer. No use for now.
     */
    public static class ProvCommitter extends OutputCommitter {

        public void abortTask(TaskAttemptContext context){}

        public void commitTask(TaskAttemptContext context){}

        public boolean needsTaskCommit(TaskAttemptContext context){return false;}

        public void setupJob(JobContext job){}

        public void setupTask(TaskAttemptContext context){}
    }

    /**
     * @author Trams Wang
     * @version 1.0
     * Date: Jan. 20, 2016
     *
     *   Customized Hadoop record writer that writes date into pipeline server.
     */
    public static class ProvRecordWriter extends RecordWriter<Integer, String> {
        private DataOutputStream out;
        private HttpURLConnection con;

        /**
         *   Create an instance of writer that will write into system output.
         */
        public ProvRecordWriter(){out = new DataOutputStream(System.out);}

        /**
         *   Create an instance of writer that will write into pipeline server denoted by surl.
         *
         * @param surl Pipeline server location.
         * @throws IOException
         */
        public ProvRecordWriter(String surl) throws IOException
        {
            URL url = new URL(surl);
            con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("PUT");
            con.setDoOutput(true);
            out = new DataOutputStream(con.getOutputStream());
        }

        /**
         *   Close output stream.
         *
         * @param context Task context.
         * @throws IOException
         */
        public void close(TaskAttemptContext context) throws IOException
        {
            out.flush();
            out.close();
            int resp_code = con.getResponseCode();
            if (200 != resp_code)
            {
                throw new IOException("Pipeline server writing failed!");
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputline;
            while (null != (inputline = in.readLine()))
            {
                System.out.println("--Response: " + inputline);
            }
        }

        /**
         *   Write one record.
         *
         * @param key Record key.
         * @param val Record value.
         * @throws IOException
         */
        public void write(Integer key, String val) throws IOException
        {
            out.write((val + '\n').getBytes());
        }
    }

    /**
     * @author Trams Wang
     * @version 1.0
     * Date: Jan. 20, 2016
     *
     *   Customized Hadoop OutputFormat that provides record writers.
     */
    public static class ProvOutputFormat extends OutputFormat {
        private String ps_location;

        /**
         *   Create an instance with default configurations.
         */
        public ProvOutputFormat()
        {
            ps_location = ProvConfig.DEF_PS_PROTOCOL + "://" + ProvConfig.DEF_PS_HOST
                    + Integer.toString(ProvConfig.DEF_PS_PORT) + '/';
        }

        public void checkOutputSpecs(JobContext context){}

        public ProvCommitter getOutputCommitter(TaskAttemptContext context)
        {
            return new ProvCommitter();
        }

        /**
         *   Provide record write.
         *
         * @param context Job context.
         * @return ProvRecordWriter instance
         * @throws IOException
         */
        public ProvRecordWriter getRecordWriter(TaskAttemptContext context) throws IOException
        {
            return new ProvRecordWriter(ps_location);
        }

        /**
         *   Set pipeline server url.
         *
         * @param url URL for connecting with pipeline server.
         */
        public void setURL(String url)
        {
            ps_location = url;
        }
    }

    private LogLine log;
    private ProvOutputFormat output_format;
    private ProvRecordWriter writer;

    /**
     *   Initiate storing procedure with some default configurations.
     */
    public ProvStorer()
    {
        log = new LogLine();
        output_format = new ProvOutputFormat();
    }

    /**
     *   Initiate storing procedure with user specified configurations.
     *
     * @param srcvar Source variable name in Pig Latin script.
     * @param srcidx Source index of srcvar.
     */
    public ProvStorer(String srcvar, String srcidx)
    {
        log = new LogLine();
        log.srcvar = srcvar;
        log.srcidx = srcidx;
        log.processor = "Storer";
        log.dstvar = "?ESServerEnd?";
        output_format = new ProvOutputFormat();
    }

    /**
     *   Provide Pig schedular with OutputFormat.
     *
     * @return ProvOutputFormat
     */
    public ProvOutputFormat getOutputFormat()
    {
        return output_format;
    }

    /**
     *   Store the infomation into log and set pipeline server address according to location string.
     *
     * @param location Location string denoted by 'host/port'.
     * @param job Job context.
     * @throws IOException
     */
    public void setStoreLocation(String location, Job job) throws IOException
    {
        output_format.setURL(ProvConfig.DEF_PS_PROTOCOL + "://" + location.replace('/', ':') + "/?log="
                + URLEncoder.encode(new Gson().toJson(log), "UTF-8"));
    }

    /**
     *   Remain location string untouched.
     *
     * @param location Origin location string 'host/port'
     * @param curDir Current directory, no use.
     * @return Location string.
     */
    public String relToAbsPathForStoreLocation(String location, Path curDir)
    {
        return location;
    }

    /**
     *   Get record writer.
     *
     * @param writer ProvRecordWriter.
     */
    public void prepareToWrite(RecordWriter writer)
    {
        this.writer = (ProvRecordWriter)writer;
    }

    /**
     *   Write one record.
     *
     * @param t Pig tuple need writing.
     * @throws IOException
     */
    public void putNext(Tuple t) throws IOException
    {
        String result = TranslateField(t);
        writer.write(0, result.substring(1, result.length()-1));
    }

    /**
     *   Translate a field object of some Pig type into string.
     *
     * @param field Object field.
     * @return Translated string.
     * @throws IOException
     */
    public static String TranslateField(Object field) throws IOException
    {
        switch (DataType.findType(field))
        {
            case DataType.NULL:
                return "";
            case DataType.BOOLEAN:
                return ((Boolean)field).toString();
            case DataType.INTEGER:
                return ((Integer)field).toString();
            case DataType.LONG:
                return ((Long)field).toString();
            case DataType.FLOAT:
                return ((Float)field).toString();
            case DataType.DOUBLE:
                return ((Double)field).toString();
            case DataType.BYTEARRAY:
                return new String(((DataByteArray)field).get());
            case DataType.CHARARRAY:
                return (String)field;
            case DataType.MAP:
            {
                boolean need_deli = false;
                Map<String, Object> m = (Map<String, Object>) field;
                String result = "" + ProvConfig.MAP_BEGIN;
                for (Map.Entry<String, Object> e : m.entrySet())
                {
                    if (need_deli)
                    {
                        result += ProvConfig.MAP_DELI;
                    }
                    else
                    {
                        need_deli = true;
                    }
                    result += e.getKey() + ProvConfig.MAP_MATCHER + TranslateField(e.getValue());
                }
                result += ProvConfig.MAP_END;
                return result;
            }
            case DataType.TUPLE:
            {
                String result = "" + ProvConfig.TUPLE_BEGIN;
                boolean need_deli = false;
                Tuple t = (Tuple)field;
                for (int i = 0; i < t.size(); i++)
                {
                    if (need_deli)
                    {
                        result += ProvConfig.TUPLE_DELI;
                    }
                    else
                    {
                        need_deli = true;
                    }
                    result += TranslateField(t.get(i));
                }
                result += ProvConfig.TUPLE_END;
                return result;
            }
            case DataType.BAG:
            {
                String result = "" + ProvConfig.BAG_BEGIN;
                boolean need_deli = false;
                Iterator<Tuple> it = ((DataBag)field).iterator();
                while(it.hasNext())
                {
                    if (need_deli)
                    {
                        result += ProvConfig.BAG_DELI;
                    }
                    else
                    {
                        need_deli = true;
                    }
                    result += TranslateField(it.next());
                }
                result += ProvConfig.BAG_END;
                return result;
            }
            default:
                throw new IOException("Unknown data type");
        }
    }
}
