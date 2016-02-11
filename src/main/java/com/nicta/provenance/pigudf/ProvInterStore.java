package com.nicta.provenance.pigudf;

import com.google.gson.Gson;
import com.nicta.provenance.ProvConfig;
import com.nicta.provenance.pipeline.LogLine;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;

/**
 * @author Trams Wang
 * @version 1.1
 * Date: Jan. 20, 2016
 *
 *   Provenance storing function for storing intermediate results. Syntax goes below:
 *   REGISTER InterStore com.nicta.provenance.pigudf.ProvInterStore('protocol', 'host', 'port');
 *   dstvar_idx = FOREACH dstvar_grp GENERATE InterStore('srcvar', 'srcvar_idx', 'processor', 'dstvar', data_content);
 *   Where, you should always initialize this function via Pig Latin macro definition, providing it with proper address
 * information about pipeline server, otherwise the function will adopt default configurations, which may sometimes be
 * wrong; 'dstvar_idx' is a relation that will contain tuples(usually only one) which each maintain only one chararray
 * field, denoting the index assigned to source data by data server; 'dstvar_grp' is a relation that groups all records
 * together from 'dstvar'; 'srcvar' is a string denotes the source variable, if there's more than one, all source
 * variable names are separated by ','; 'srcvar_idx' is a string denotes the source variable index, if there's more than
 * one source variable, all indices should be separated by ',' and be one-to-one corresponding to variables in 'srcvar';
 * 'processor' stands for the user specified name for this procedure; 'dstvar' is the name of the variable user would
 * like to store data from; 'data_content' is a bag contains all tuples that need storing.
 *   However, the function shall never be used alone. To transmit entire relation referred by some variable 'var', it
 * should always accompany a code block, as shown in the example below:
 *   E.g.
 *   REGISTER InterStore com.nicta.provenance.pigudf.ProvInterStore('http', 'localhost', '8888');
 *   ...
 *   dstvar = JOIN srcvar1 BY $0, srcvar2 BY $0;
 *   dstvar_grp = GROUP dstvar ALL;
 *   dstvar_idx = FOREACH dstvar_grp GENERATE InterStore('srcvar1,srcvar2', 'srcvar1_idx,srcvar2_idx', 'Example',
 *                                                       'dstvar', dstvar_grp.dstvar);
 *   ...
 */
public class ProvInterStore extends EvalFunc<String>{
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
     *   Main body of storing procedure. Fields in 'input' should be:
     *   $0: 'srcvars'
     *   $1: 'srcvars_idx'
     *   $2: 'processor'
     *   $3: 'dstvar'
     *   $4: Data bag
     *
     * @param input Source data
     * @return Index assigned to those data by data server.
     * @throws IOException
     */
    public String exec(Tuple input) throws IOException
    {
        LogLine log = new LogLine();
        log.srcvar = (String)input.get(0);
        log.srcidx = (String)input.get(1);
        log.processor = (String)input.get(2);
        log.dstvar = (String)input.get(3);
        String loginfo = new Gson().toJson(log);

        URL url = new URL(protocol + "://" + ps_host + ':' + Integer.toString(ps_port)
                + "/?log=" + URLEncoder.encode(loginfo, "UTF-8"));
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setDoOutput(true);
        con.setDoInput(true);
        con.setRequestMethod("PUT");
        OutputStream out = con.getOutputStream();

        DataBag bag = (DataBag)input.get(4);
        for (Tuple t : bag)
        {
            String result = ProvStorer.TranslateField(t);
            out.write(((result.substring(1, result.length()-1)) + '\n').getBytes());
        }
        out.close();

        int resp_code = con.getResponseCode();
        if (200 != resp_code)
        {
            throw new IOException("Intermediate data storing failed!");
        }
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String dstidx = in.readLine();
        in.close();
        return dstidx;
    }
}
