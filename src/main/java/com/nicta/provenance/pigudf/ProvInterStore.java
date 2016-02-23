package com.nicta.provenance.pigudf;

import com.google.gson.Gson;
import com.nicta.provenance.ProvConfig;
import com.nicta.provenance.pipeline.LogLine;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * @author Trams Wang
 * @version 2.0
 * Date: Feb. 12, 2016
 *
 *   Provenance storing function for storing intermediate results. Syntax goes below:
 *
 *   REGISTER InterStore com.nicta.provenance.pigudf.ProvInterStore('protocol', 'host', 'port');
 *   dstvar = FOREACH dstvar GENERATE FLATTEN(InterStore('srcvar', 'processor', 'dstvar', *));
 *
 *   Where, you should always initialize this function via Pig Latin macro definition, providing it with proper address
 * information about pipeline server, otherwise the function will adopt default configurations, which may sometimes be
 * wrong; 'dstvar' is the relation that's going to be stored, we suggest using same relation name when applying this
 * operation for the sake of performance and correctness; 'srcvar' is a string denotes the source variable, if there's
 * more than one, all source variable names are separated by ','; 'processor' stands for the user specified name for
 * this procedure; 'dstvar' is the name of the variable user would like to store data from; last asterisk is compulsory
 * for it means the entire data content.
 *
 *   This function shall always be used inside a FLATTEN operator, as shown in the example below:
 *   E.g.
 *   REGISTER InterStore com.nicta.provenance.pigudf.ProvInterStore('http', 'localhost', '8888');
 *   ...
 *   dstvar = JOIN srcvar1 BY $0, srcvar2 BY $0;
 *   dstvar = FOREACH dstvar GENERATE FLATTEN(InterStore('srcvar1,srcvar2', 'Example', 'dstvar', *));
 *   ...
 */
public class ProvInterStore extends EvalFunc<Tuple>{
    private String protocol;
    private String ps_host;
    private int ps_port;

    /**
     *   Initiate storing procedure with default configurations.
     */
    public ProvInterStore()
    {
        protocol = ProvConfig.DEF_PS_PROTOCOL;
        ps_host = ProvConfig.DEF_PS_HOST;
        ps_port = ProvConfig.DEF_PS_PORT;
    }

    /**
     *   Initiate storing procedure with user specified configurations.
     *
     * @param protocol Protocol to communicate with pipeline server
     * @param ps_host Pipeline server host
     * @param ps_port Pipeline server port
     */
    public ProvInterStore(String protocol, String ps_host, String ps_port)
    {
        this.protocol = protocol;
        this.ps_host = ps_host;
        this.ps_port = Integer.parseInt(ps_port);
    }

    /**
     *   Main body of storing procedure. Store one record line at a time. Fields in 'input' should be:
     *   $0: 'srcvars'
     *   $1: 'processor'
     *   $2: 'dstvar'
     *   $3,...: record content
     *
     * @param input Source data
     * @return Stored tuple.
     * @throws IOException
     */
    public Tuple exec(Tuple input) throws IOException
    {
        //System.out.println("-----------------------INTERSTORE");
        LogLine log = new LogLine();
        log.srcvar = (String)input.get(0);
        log.processor = (String)input.get(1);
        log.dstvar = (String)input.get(2);
        String loginfo = new Gson().toJson(log);

        URL url = new URL(protocol + "://" + ps_host + ':' + Integer.toString(ps_port)
                + "/?log=" + URLEncoder.encode(loginfo, "UTF-8"));
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod("PUT");
        con.setDoOutput(true);
        con.setDoInput(true);
        OutputStream out = con.getOutputStream();
        int len = input.size();
        String result = "";
        Tuple tuple = TupleFactory.getInstance().newTuple();
        for (int i = 3; i < len; i++)
        {
            result += ProvStorer.TranslateField(input.get(i));
            if (i != len - 1) result += ',';
            tuple.append(input.get(i));
        }
        out.write((result + '\n').getBytes());
        out.close();
        int resp_code = con.getResponseCode();
        if (200 != resp_code)
        {
            throw new IOException("Intermediate data storing failed!");
        }
        return tuple;
    }
}
