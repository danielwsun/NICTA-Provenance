package com.nicta.provenance.query;

import com.google.gson.Gson;
import com.nicta.provenance.ProvConfig;
import com.nicta.provenance.pipeline.ESSlave;
import com.nicta.provenance.pipeline.LogLine;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;

/**
 * @author Trams Wang
 * @version 2.0
 * Date: Feb. 23, 2016
 *
 *   Standalone server for interacting with users. Is able to query for:
 *   (1) Intermediate/final result data content;
 *   (2) Meta data of pipeline operation;
 *   (3) Pipeline semantics.
 *
 * Protocol:
 *   Context: /_help
 *     GET: Get help instruction.
 *   Context: /_data
 *     GET: Get data content.
 *   Context: /_meta
 *     GET: Get pipeline operation meta info.
 *   Context: /_semantics
 *     GET: View pipeline structure and operation details.
 */
public class QueryServer {

    /**
     * @author Trams Wang
     * @version 1.0
     * Date: Feb. 23, 2016
     *
     *   Handler class to deal with data query. Handle only 'GET' method.
     */
    private class DataHandler implements HttpHandler{
        public DataHandler(){}
        public void handle(HttpExchange t)
        {
            try
            {
                String method = t.getRequestMethod();
                if ("GET".equals(method))
                {
                    handleGet(t);
                } else
                {
                    handleOtherMethods(t);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        /**
         *    'GET' method handler.
         *    Handle request URI of the following form: /query_expression. Return all matched data content.
         *
         * @param t Http context
         * @throws IOException
         * @throws JSONException
         */
        private void handleGet(HttpExchange t) throws IOException, JSONException
        {
            String query = t.getRequestURI().toString().substring(1);
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();

            String json = builder.build(query);
            String ans = esslave.simpleHttpRequest(esslave.getESSLocation() + "_search", "GET", json);

            JSONArray hits = new JSONObject(ans).getJSONObject("hits").getJSONArray("hits");
            int len = hits.length();
            for (int i = 0; i < len; i++)
            {
                JSONObject obj = hits.getJSONObject(i);
                String idx = obj.getJSONObject("_source").getString("dstidx");
                out.write(String.format("%d: \n", i).getBytes());

                URL url = new URL(ds_location + idx);
                HttpURLConnection con = (HttpURLConnection)url.openConnection();
                con.setRequestMethod("GET");
                con.setDoInput(true);
                con.setDoOutput(false);

                int respcode = con.getResponseCode();
                if (400 == respcode)
                {
                    out.write("DS connection failed\n".getBytes());
                    continue;
                }
                BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
                String inputline;
                while (null != (inputline = in.readLine()))
                {
                    out.write((inputline + '\n').getBytes());
                }
                in.close();
            }
            out.close();
        }

        private void handleOtherMethods(HttpExchange t) throws IOException
        {
            String info = "Method Not Implemented!\n";
            t.sendResponseHeaders(404, info.length());
            OutputStream out = t.getResponseBody();
            out.write(info.getBytes());
            out.close();
        }
    }

    /**
     * @author Trams Wang
     * @version 1.0
     * Date: Feb. 23, 2016
     *
     *   Handler class to deal with semantics query. Handle only 'GET' method.
     */
    private class SemanticsHandler implements HttpHandler{
        public SemanticsHandler(){}
        public void handle(HttpExchange t)
        {
            try
            {
                String method = t.getRequestMethod();
                if ("GET".equals(method))
                {
                    handleGet(t);
                } else
                {
                    handleOtherMethods(t);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        /**
         *   'GET' method handler.
         *   Handle request URI of the following form: /query_string. Return the pipeline information.
         *
         * @param t Http context.
         * @throws IOException
         * @throws JSONException
         */
        private void handleGet(HttpExchange t) throws IOException, JSONException
        {
            String query = t.getRequestURI().toString().substring(1);

            URL url = new URL(ps_location + ProvConfig.DEF_PS_SEM_CONTEXT);
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("GET");
            con.setDoOutput(false);
            con.setDoInput(true);

            int respcode = con.getResponseCode();
            if (400 == respcode) throw new IOException("Pipeline server error");
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String inputline;
            while (null != (inputline = in.readLine()))
            {
                out.write((inputline + '\n').getBytes());
            }
            in.close();
            out.close();
        }

        private void handleOtherMethods(HttpExchange t) throws IOException
        {
            String info = "Method Not Implemented!\n";
            t.sendResponseHeaders(404, info.length());
            OutputStream out = t.getResponseBody();
            out.write(info.getBytes());
            out.close();
        }
    }

    /**
     * @author Trams Wang
     * @version 1.0
     * Date: Feb. 23, 2016
     *
     *   Handler class to deal with meta information query. Handle only 'GET' method.
     */
    private class MetaHandler implements HttpHandler{
        private String format_string = "%-14.14s %-14.14s %-14.14s %-14.14s %-14.14s\n";
        public MetaHandler(){}
        public void handle(HttpExchange t)
        {
            try
            {
                String method = t.getRequestMethod();
                if ("GET".equals(method))
                {
                    handleGet(t);
                } else
                {
                    handleOtherMethods(t);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
            }
        }

        /**
         *   'GET' method handler.
         *   Handle request URI of the following form: /query_string. Return matched operation detail.
         *
         * @param t Http context.
         * @throws IOException
         * @throws JSONException
         */
        private void handleGet(HttpExchange t) throws IOException, JSONException
        {
            String query = t.getRequestURI().toString().substring(1);
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();
            out.write(String.format(format_string,
                    "Srcvar", "Srcidx", "Processor", "Dstvar", "Dsridx").getBytes());

            String json = builder.build(query);
            String ans = esslave.simpleHttpRequest(esslave.getESSLocation() + "_search", "GET", json);

            JSONArray hits = new JSONObject(ans).getJSONObject("hits").getJSONArray("hits");
            int len = hits.length();
            for (int i = 0; i < len; i++)
            {
                JSONObject obj = hits.getJSONObject(i);
                LogLine log = gson.fromJson(obj.getString("_source"), LogLine.class);
                out.write(String.format(format_string,
                        log.srcvar, log.srcidx, log.processor, log.dstvar, log.dstidx).getBytes());
                        /*(null == log.srcvar)?"NULL":log.srcvar,
                        (null == log.srcidx)?"NULL":log.srcidx,
                        (null == log.processor)?"NULL":log.processor,
                        (null == log.dstvar)?"NULL":log.dstvar,
                        (null == log.dstidx)?"NULL":log.dstidx).getBytes());*/
            }
            out.close();
        }

        private void handleOtherMethods(HttpExchange t) throws IOException
        {
            String info = "Method Not Implemented!\n";
            t.sendResponseHeaders(404, info.length());
            OutputStream out = t.getResponseBody();
            out.write(info.getBytes());
            out.close();
        }
    }

    /**
     * @author Trams Wang
     * @version 1.0
     * Date: Feb. 23, 2016
     *
     *   Handler class to deal with help request. Handle only 'GET' method.
     */
    private class HelpHandler implements HttpHandler{
        public HelpHandler(){}
        public void handle(HttpExchange t)
        {
            try
            {
                String method = t.getRequestMethod();
                if ("GET".equals(method))
                {
                    handleGet(t);
                } else
                {
                    handleOtherMethods(t);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }

        /**
         *   'GET' method handler.
         *   No URI paras required. Return help document.
         *
         * @param t Http context.
         * @throws IOException
         */
        private void handleGet(HttpExchange t) throws IOException
        {
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();
            out.write("Help:\n".getBytes());
            out.write("\t1. Help: /help\n".getBytes());
            out.write("\t2. Query for data: /data\n".getBytes());
            out.write("\t3. Query for meta data: /meta\n".getBytes());
            out.write("\t4. Query for pipeline structure: /semantics\n".getBytes());
            out.close();
        }

        private void handleOtherMethods(HttpExchange t) throws IOException
        {
            String info = "Method Not Implemented!\n";
            t.sendResponseHeaders(404, info.length());
            OutputStream out = t.getResponseBody();
            out.write(info.getBytes());
            out.close();
        }
    }

    private String ps_location;
    private String ds_location;
    private QueryBuilder builder;
    private ESSlave esslave;
    private Gson gson;

    public QueryServer() throws IOException
    {
        ps_location = ProvConfig.DEF_PS_PROTOCOL + "://" + ProvConfig.DEF_PS_HOST + ':'
                + Integer.toString(ProvConfig.DEF_PS_PORT) + '/';
        ds_location = ProvConfig.DEF_DS_PROTOCOL + "://" + ProvConfig.DEF_DS_HOST + ':'
                + Integer.toString(ProvConfig.DEF_DS_PORT) + '/';
        builder = new QueryBuilder();
        esslave = new ESSlave();
        gson = new Gson();
    }

    public void initiate() throws IOException
    {
        System.out.println("QueryServer Initiating...");
        System.out.println("\tCommunicate with PipeServer at: " + ps_location);
        System.out.println("\tCommunicate with DataServer at: " + ds_location);
        System.out.println("\tCommunicate with ESServer at: " + esslave.getESSLocation());
        HttpServer server = HttpServer.create(new InetSocketAddress(ProvConfig.DEF_QS_HOST, ProvConfig.DEF_QS_PORT), 0);
        HttpContext data_context = server.createContext(ProvConfig.DEF_QS_DATA_CONTEXT, new DataHandler());
        HttpContext sem_context = server.createContext(ProvConfig.DEF_QS_SEM_CONTEXT, new SemanticsHandler());
        HttpContext meta_context = server.createContext(ProvConfig.DEF_QS_META_CONTEXT, new MetaHandler());
        HttpContext help_context = server.createContext(ProvConfig.DEF_QS_HELP_CONTEXT, new HelpHandler());
        server.setExecutor(null);
        server.start();
        System.out.println("QueryServer Initiated");
    }
}
