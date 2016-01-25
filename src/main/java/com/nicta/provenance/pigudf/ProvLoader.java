package com.nicta.provenance.pigudf;

import com.google.gson.Gson;
import com.nicta.provenance.ProvConfig;
import com.nicta.provenance.pipeline.LogLine;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.*;
import org.apache.pig.LoadFunc;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Trams Wang
 * @version 1.2
 * Date: Jan. 19, 2016
 *
 *   Provenance loading function. Syntax for using this function in Pig Latin goes below:
 *   varname = LOAD 'host/port' USING com.nicta.provenance.pigudf.ProvLoader('srcidx', 'varname') AS ...;
 *   Where 'ps_host' and 'ps_port' are host address and port for pipeline server; 'varname' is the name of the
 * variable that receives the result provided by LOAD; 'srcidx' is the index of source data.
 */
public class ProvLoader extends LoadFunc {

    /**
     * @author Trams Wang
     * @version 1.1
     * Date: Jan. 20, 2016
     *
     *   Customized Hadoop split class that works for ProvLoader.
     */
    public static class ProvSplit extends InputSplit implements Writable {
        private String ps_location;

        /**
         *   Create an instance with default configurations.
         */
        public ProvSplit()
        {
            ps_location = ProvConfig.DEF_PS_PROTOCOL + "://" + ProvConfig.DEF_PS_HOST + ':'
            + Integer.toString(ProvConfig.DEF_PS_PORT) + '/';
        }

        /**
         *   Create an instance with user specified configuration.
         * @param ps_url User specified URL used to connect with pipeline server.
         */
        public ProvSplit(String ps_url)
        {
            ps_location = ps_url;
        }

        public long getLength()
        {
            return 0;
        }

        public String[] getLocations()
        {
            String[] locations = new String[1];
            locations[0] = ps_location;
            return locations;
        }

        public void write(DataOutput out) throws IOException
        {
            out.writeUTF(ps_location);
        }

        public void readFields(DataInput in) throws IOException
        {
            ps_location = in.readUTF();
        }
    }

    /**
     * @author Trams Wang
     * @version 1.1
     * Date: Jan. 20, 2016
     *
     *   Customized Hadoop reader that reads data from pipeline server.
     */
    public static class ProvRecordReader extends RecordReader<Integer,String> {
        private BufferedReader reader;
        private String cur;
        private String ps_location;

        public ProvRecordReader()
        {
            reader = null;
            cur = null;
            ps_location = ProvConfig.DEF_PS_PROTOCOL + "://" + ProvConfig.DEF_PS_HOST + ';'
                    + Integer.toString(ProvConfig.DEF_PS_PORT) + '/';
        }

        /**
         *   Connect to pipeline server and prepare the input stream.
         *
         * @param split ProvSplit instance.
         * @param context Haddop task context.
         * @throws InterruptedException
         * @throws IOException
         */
        public void initialize(InputSplit split, TaskAttemptContext context) throws InterruptedException, IOException
        {
            /* Connect to pipeline server*/
            ps_location = split.getLocations()[0];
        }

        /**
         *   Check if there's more content to read.
         *
         * @return True if there's another record waiting to be read; false otherwise.
         * @throws IOException
         */
        public boolean nextKeyValue() throws IOException
        {
            if (null == reader)
            {
                URL url = new URL(ps_location);
                HttpURLConnection con = (HttpURLConnection)url.openConnection();
                con.setRequestMethod("GET");
                con.setDoInput(true);
                int response_code = con.getResponseCode();
                if (200 != response_code)
                {
                    throw new IOException("Pipeline Server Connection Failed!");
                }
                reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
            }
            cur = reader.readLine();
            return null != cur;
        }

        /**
         *   Get key of the current record.
         *   No key is involved for now.
         *
         * @return 0
         */
        public Integer getCurrentKey()
        {
            return 0;
        }

        /**
         *   Get value of the current record.
         *
         * @return Current record value.
         */
        public String getCurrentValue()
        {
            return cur;
        }

        /**
         *   Get progress of current job.
         *
         * @return 0
         */
        public float getProgress()
        {
            return 0.0f;
        }

        /**
         *   Close reader.
         *
         * @throws IOException
         */
        public void close() throws IOException
        {
            reader.close();
        }
    }

    /**
     * @author Trams Wang
     * @version 1.1
     * Date: Jan. 20, 2016
     *
     *   Customized Hadoop InputFormat that generates split plan and provide record reader according to split info.
     */
    public static class ProvInputFormat extends InputFormat {
        private String ps_location;

        /**
         *   Create an instance with default configurations.
         */
        public ProvInputFormat()
        {
            ps_location = ProvConfig.DEF_PS_PROTOCOL + "://" + ProvConfig.DEF_PS_HOST + ':'
                     + Integer.toString(ProvConfig.DEF_PS_PORT) + '/';
        }

        /**
         *   Create an instance with user specified configuration.
         *
         * @param ps_url User specified URL used to connect with pipeline server.
         */
        public ProvInputFormat(String ps_url)
        {
            ps_location = ps_url;
        }

        /**
         *   Create a ProvRecordReader according to job specification.
         *
         * @param split ProvSplit.
         * @param context Task context.
         * @return New instance of ProvRecordReader used to read data from pipeline server.
         * @throws InterruptedException
         * @throws IOException
         */
        public ProvRecordReader createRecordReader(InputSplit split, TaskAttemptContext context)
                throws InterruptedException, IOException
        {
            ProvRecordReader reader = new ProvRecordReader();
            reader.initialize(split, context);
            return reader;
        }

        /**
         *   Generate a job split plan.
         *
         * @param job Job context.
         * @return List of splits.
         */
        public List<InputSplit> getSplits(JobContext job)
        {
            ArrayList<InputSplit> list =  new ArrayList<InputSplit>();
            list.add(new ProvSplit(ps_location));
            return list;
        }

        /**
         *   Set pipeline URL.
         *
         * @param ps_url User specified URL used to connect with pipeline server.
         */
        public void setURL(String ps_url)
        {
            ps_location = ps_url;
        }
    }

    private ProvInputFormat input_format;
    private ProvRecordReader reader;
    private LogLine log;

    /**
     *   Initiate the loading procedure with default pipeline server address.
     */
    public ProvLoader()
    {
        input_format = new ProvInputFormat();
        log = new LogLine();
    }

    /**
     *   Initiate the loading procedure with user specified configuration.
     *
     * @param srcidx Source index denotes the source data.
     * @param dstvar Pig Latin variable name.
     */
    public ProvLoader(String srcidx, String dstvar)
    {
        input_format = new ProvInputFormat();
        log = new LogLine();
        log.srcvar = "ESServerStart";
        log.srcidx = "";
        log.processor = dstvar + "Loader";
        log.dstvar = dstvar;
        log.dstidx = srcidx;
    }

    /**
     *   Provide Pig scheduler the InputFormat instance.
     *
     * @return An instance of ProvInputFormat used by ProvLoader.
     * @see com.nicta.provenance.pigudf.ProvLoader.ProvInputFormat
     */
    public InputFormat getInputFormat() {return input_format;}

    /**
     *   Remain location string untouched.
     *
     * @param location Origin location string 'host/port'
     * @param curDir Current directory, no use.
     * @return Location string.
     */
    /* Remain the index as what user specified in Pig Latin script*/
    public String relativeToAbsolutePath(String location, Path curDir)
    {
        return location;
    }

    /**
     *   Get pipeline server location.
     *
     * @param location Pipeline location string, 'host/port'.
     * @param job Job context.
     */
    /* May be called multiple times from both front and back end.*/
    public void setLocation(String location, Job job) throws IOException
    {
        input_format.setURL(ProvConfig.DEF_PS_PROTOCOL + "://" + location.replace('/', ':') + "/?log="
                + URLEncoder.encode(new Gson().toJson(log), "UTF-8"));
    }

    /**
     *   Get ProvRecordReader to read data.
     *
     * @param reader ProvRecordReader
     * @param split Job split info
     */
    /* Called in each map task*/
    public void prepareToRead(RecordReader reader, PigSplit split) {this.reader = (ProvRecordReader)reader;}

    /**
     *   Read one record at one time.
     *   No complex types and nested types supported for now.
     *
     * @return Pig tuple constructed from one record.
     * @throws IOException
     */
    public Tuple getNext() throws IOException
    {
        String val;
        if (!reader.nextKeyValue()) return null;
        val = reader.getCurrentValue();
        int len = val.length();
        int start = 0;
        ArrayList<Object> list = new ArrayList<Object>();
        for (int i = 0; i < len; i++)
        {
            if (ProvConfig.TUPLE_DELI == val.charAt(i))
            {
                list.add(new DataByteArray(val.substring(start, i)));
                start = i + 1;
            }
        }
        list.add(new DataByteArray(val.substring(start)));
        Tuple t = TupleFactory.getInstance().newTupleNoCopy(list);
        return t;
    }
}
