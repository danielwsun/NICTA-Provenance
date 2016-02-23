package com.nicta.provenance.pipeline;


import com.nicta.provenance.ProvConfig;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @author Trams Wang
 * @version 1.1
 * Date: Jan. 19, 2016
 *
 *   This class is used by PipeServer to communicate with Elastic Search Server.
 */
public class ESSlave{
    private String es_index;
    private String es_type;
    private String ess_location;
    private String mapping_json;

    /**
     *   Create an instance with default configurations.
     *
     * @throws IOException
     */
    public ESSlave() throws IOException
    {
        es_index = ProvConfig.DEF_ESS_INDEX;
        es_type = ProvConfig.DEF_ESS_TYPE;
        ess_location = ProvConfig.DEF_ESS_PROTOCOL + "://" + ProvConfig.DEF_ESS_HOST + ':'
                + Integer.toString(ProvConfig.DEF_ESS_PORT) + '/';
        BufferedReader in = new BufferedReader(new FileReader(ProvConfig.DEF_ESS_MAPPING_FILE));
        mapping_json = in.readLine();
        in.close();
    }

    /**
     *   Create an instance with user specified configurations.
     *
     * @param host Host of ES server.
     * @param port Port of ES server.
     * @param index Index used by provenance system.
     * @param type Type used by provenance system.
     * @throws FileNotFoundException
     * @throws IOException
     */
    public ESSlave(String host, int port, String index, String type) throws IOException
    {
        es_index = index;
        es_type = type;
        ess_location = ProvConfig.DEF_ESS_PROTOCOL + "://" + host + '/'
                + Integer.toString(port) + '/';
        BufferedReader in = new BufferedReader(new FileReader(ProvConfig.DEF_ESS_MAPPING_FILE));
        mapping_json = in.readLine();
        in.close();
    }

    /**
     *   Send to ES server a json document.
     *
     * @param json Document that will be send to ES server.
     * @throws IOException
     */
    public void send(String json) throws IOException
    {
        System.out.println(simpleHttpRequest(ess_location + es_index + '/' + es_type, "POST", json));
    }

    /**
     *   Delete all records in the specified ES index/type.
     *
     * @throws IOException
     */
    public void cleanUp() throws IOException
    {
        System.out.println(simpleHttpRequest(ess_location + es_index + '/' + es_type, "DELETE", null));
    }

    /**
     *   Delete ES index/type and then recreate one.
     *
     * @throws IOException
     */
    public void rebuild() throws IOException
    {
        System.out.println(simpleHttpRequest(ess_location + es_index, "DELETE", null));
        System.out.println(simpleHttpRequest(ess_location + es_index, "PUT", null));
        System.out.println(simpleHttpRequest(ess_location + es_index + "/_mapping" + '/' + es_type,
                "PUT", mapping_json));
    }

    /**
     *    Send ES server some message (maybe also some files).
     *
     * @param str_url Specify the target URL.
     * @param method Http method.
     * @param content If additional file is needed, add the content string here; otherwise pass a null.
     * @return ES server response.
     * @throws IOException
     */
    public String simpleHttpRequest(String str_url, String method, String content) throws IOException
    {
        URL url = new URL(str_url);
        HttpURLConnection con = (HttpURLConnection)url.openConnection();
        con.setRequestMethod(method);
        con.setDoInput(true);
        if (null == content)
        {
            con.setDoOutput(false);
        }
        else
        {
            con.setDoOutput(true);
            OutputStream out = con.getOutputStream();
            out.write(content.getBytes());
            out.close();
        }

        int resp_code = con.getResponseCode();
        BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
        String inputline;
        String resp = "";
        while (null != (inputline = in.readLine()))
        {
            resp += inputline;
        }
        System.out.println("ES Resp: " + Integer.toString(resp_code));
        return resp;
    }

    public String getESSLocation() {return ess_location + es_index + '/' + es_type;}
}
