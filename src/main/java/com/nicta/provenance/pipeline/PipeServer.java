package com.nicta.provenance.pipeline;

import com.google.gson.Gson;
import com.nicta.provenance.ProvConfig;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

/**
 * @author Trams Wang
 * @version 2.0
 * Date: Feb. 12, 2016
 *
 *   Standalone server for pipeline monitoring, including semantics inferring, operation scoring and data dispatching.
 * It sends intermediate/final result data to data server and sends log records to ES server. At mean time, it infers
 * and stores pipeline semantics locally, score all pipeline paths after storing the final result.
 *
 * Protocol:
 *   Context: /
 *     GET: Fetch data from data server.
 *     PUT: Pass intermediate results to DS and infer pipeline structure.
 *     POST: Pass final results to DS and score the path.
 *   Context: /_semantics
 *     GET: View pipeline semantics structure and operation scores.
 *
 * @see com.nicta.provenance.pipeline.PipeServer.DataHandler for ptorocol detail.
 */
public class PipeServer {

    /**
     * @author Trams Wang
     * @version 1.1
     * Date: Feb. 12, 2016
     *
     *   This class is used for buffering connection and incomplete log info.
     */
    private class Tunnel{
        public LogLine log;
        public HttpURLConnection con;
        public boolean closed;

        public Tunnel()
        {
            log = null;
            con = null;
            closed = false;
        }

        /**
         *   Close buffer and complete log info.
         *
         * @return Index for stored data.
         * @throws IOException
         */
        public String close() throws IOException
        {
            if (closed) return log.dstidx;
            closed = true;
            con.getOutputStream().close();
            int respcode = con.getResponseCode();
            if (400 == respcode) throw new IOException("Data server Error");
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));
            String dstidx = in.readLine();
            in.close();
            log.dstidx = dstidx;
            return dstidx;
        }
    }

    /**
     * @author Trams Wang
     * @version  2.0
     * Date Feb. 12, 2016
     *
     *   Handle data requests for PipeServer. Handles ONLY 'GET', 'PUT' and 'POST' methods.
     */
    private class DataHandler implements HttpHandler{

        public DataHandler() {}

        /**
         *   Request dispatcher.
         *
         * @param t HTTP context.
         */
        public void handle(HttpExchange t)
        {
            try
            {
                String method = t.getRequestMethod();
                //System.out.println("URL: " + t.getRequestURI());
                //System.out.println("Method : " + method);
                if ("GET".equals(method))
                {
                    handleGet(t);
                }
                else if ("PUT".equals(method))
                {
                    handlePut(t);
                }
                else if ("POST".equals(method))
                {
                    handlePost(t);
                }
                else
                {
                    handleOtherMethods(t);
                }
            }
            catch (Exception e)
            {
                e.printStackTrace();
                /*try
                {
                    String info = "Error In Pipeline Server";
                    t.sendResponseHeaders(400, info.length());
                    OutputStream out = t.getResponseBody();
                    out.write(info.getBytes());
                    out.close();
                }
                catch (IOException ee)
                {
                    System.out.println("Pipeline Server Nested Error.");
                    ee.printStackTrace();
                }*/
            }
        }

        /**
         *   'GET' method handler.
         *   Handle 'GET' request URI of the following form: /?log=encoded_json_string, where 'encoded_json_string' is
         * converted and encoded from a LogLine instance. 'dstidx' and 'dstvar' shall be filled in in the parameter.PS
         * will fetche from data server the content of 'dstidx'. Note: LogLine.dstidx here denotes the index that
         * refers to the source data.
         *
         * @param t HTTP context.
         * @throws IOException
         */
        private void handleGet(HttpExchange t) throws IOException
        {
            /* Decode log info*/
            String log_json = URLDecoder.decode(t.getRequestURI().toString().substring(6), "UTF-8");
            Gson gson = new Gson();
            LogLine log = gson.fromJson(log_json, LogLine.class);

            /* Send DataServer url to client*/
            String dsurl = data_server_location + log.dstidx;
            t.sendResponseHeaders(200, dsurl.getBytes().length);
            OutputStream out = t.getResponseBody();
            out.write(dsurl.getBytes());
            out.close();

            /* Store log info into ES server*/
            es_slave.send(log_json);
            inferer.absorb(log.srcvar, log.processor, log.dstvar);
            System.out.println("PS:: GET DONE");
        }

        /**
         *   'PUT' method handler.
         *   Handle 'PUT' request URI of the following form: /?log=encoded_json_string, where 'encoded_json_string' is
         * converted and encoded from a LogLine instance. These members shall be already assigned: srcvar, processor
         * and dstvar. PS buffers the connection and log info and complete pipeline structure by one piece of operation.
         *
         * @param t HTTP context.
         * @throws IOException
         * @see LogLine
         */
        private void handlePut(HttpExchange t) throws IOException
        {
            /* Parse the log para*/
            String log_json = URLDecoder.decode(t.getRequestURI().toString().substring(6), "UTF-8");
            Gson gson = new Gson();
            LogLine log = gson.fromJson(log_json, LogLine.class);

            Tunnel tunnel = tun_pool.get(log.dstvar);
            if (null == tunnel)
            {
                URL url = new URL(data_server_location + log.dstvar);
                HttpURLConnection con = (HttpURLConnection)url.openConnection();
                con.setDoOutput(true);
                con.setDoInput(true);
                con.setRequestMethod("PUT");

                tunnel = new Tunnel();
                tunnel.con = con;
                tunnel.log = log;

                tun_pool.put(log.dstvar, tunnel);
            }
            OutputStream out = tunnel.con.getOutputStream();


            BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
            String inputline;
            while (null != (inputline = in.readLine()))
            {
                out.write((inputline+'\n').getBytes());
            }
            in.close();
            t.sendResponseHeaders(200, -1);
            t.getResponseBody().close();
        }

        /**
         *   'POST' method handler.
         *   Handle 'PUT' request URI of the following form: /?log=encoded_json_string, where 'encoded_json_string' is
         * converted and encoded from a LogLine instance. These members shall be already assigned: srcvar, processor
         * and dstvar. PS will flush all data into DS and complete log info then store the log info into ESS. Finally,
         * it will ask oracle about the judgement for final result and score relative paths.
         *
         * @param t HTTP context.
         * @throws IOException
         */
        private void handlePost(HttpExchange t) throws IOException
        {
            /* Parse the log para*/
            String log_json = URLDecoder.decode(t.getRequestURI().toString().substring(6), "UTF-8");
            Gson gson = new Gson();
            LogLine log = gson.fromJson(log_json, LogLine.class);

            Tunnel tunnel = tun_pool.get(log.dstvar);
            if (null == tunnel)
            {
                URL url = new URL(data_server_location + log.dstvar);
                HttpURLConnection con = (HttpURLConnection)url.openConnection();
                con.setDoOutput(true);
                con.setDoInput(true);
                con.setRequestMethod("PUT");

                tunnel = new Tunnel();
                tunnel.con = con;
                tunnel.log = log;

                tun_pool.put(log.dstvar, tunnel);
            }
            OutputStream out = tunnel.con.getOutputStream();

            /* Send data to scoring server*/
            URL ans_url = new URL("http://localhost:6666");
            HttpURLConnection ans_con = (HttpURLConnection)ans_url.openConnection();
            ans_con.setDoOutput(true);
            ans_con.setDoInput(true);
            ans_con.setRequestMethod("GET");
            OutputStream ans_out = ans_con.getOutputStream();

            BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
            String inputline;
            while (null != (inputline = in.readLine()))
            {
                ans_out.write((inputline+'\n').getBytes());
                out.write((inputline+'\n').getBytes());
            }
            in.close();
            t.sendResponseHeaders(200, 0);
            t.getResponseBody().close();

            closeTunnel(log.dstvar);

            LogLine end_log = new LogLine();
            end_log.srcvar = log.dstvar;
            end_log.srcidx = log.dstidx;
            end_log.processor = log.dstvar + "_Storer";
            es_slave.send(gson.toJson(end_log));
            inferer.absorb(end_log.srcvar, end_log.processor, end_log.dstvar);

            /* Score data according to user's specification*/
            System.out.println("Checking for similarity...");
            int resp_code = ans_con.getResponseCode();
            if (400 == resp_code)
            {
                throw new IOException("Answer Testing Server Error.");
            }
            Scanner scanner = new Scanner(ans_con.getInputStream());
            double ans = scanner.nextDouble();
            System.out.println("Similarity is: " + Double.toString(ans));
            scorer.scorePath(inferer.dataNode(log.dstvar), ans);
        }

        /**
         *   Close buffer recursively.
         *
         * @param var Variable name that is being stored.
         * @return Index for the variable 'var'.
         * @throws IOException
         */
        private String closeTunnel(String var) throws IOException
        {
            Tunnel t = tun_pool.get(var);
            if (null == t) return null;
            String [] srcvars = t.log.srcvar.split(",");
            String srcidx = "";
            boolean first = true;
            for (String s : srcvars)
            {
                String idx = closeTunnel(s);
                if (null != idx)
                {
                    srcidx += (first)?idx:(','+idx);
                    first = false;
                }
            }
            t.log.srcidx = ("".equals(srcidx))?null:srcidx;
            t.close();
            tun_pool.remove(var);
            es_slave.send(new Gson().toJson(t.log));
            inferer.absorb(t.log.srcvar, t.log.processor, t.log.dstvar);
            return t.log.dstidx;
        }

        /**
         *   Handle other unimplemented methods.
         *
         * @param t
         * @throws IOException
         */
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
     * Date: Jan. 27, 2016
     *
     *   Handle semantics requests for PipeServer. Handles ONLY 'GET' method.
     */
    private class SemanticsHandler implements HttpHandler{
        public SemanticsHandler() {}

        public void handle(HttpExchange t)
        {
            try
            {
                String method = t.getRequestMethod();
                if ("GET".equals(method))
                {
                    handleGet(t);
                }
                else
                {
                    handleOtherMethods(t);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
                try
                {
                    String info = "Error In Pipeline Server";
                    t.sendResponseHeaders(400, info.length());
                    OutputStream out = t.getResponseBody();
                    out.write(info.getBytes());
                    out.close();
                }
                catch (IOException ee)
                {
                    System.out.println("Pipeline Server Nested Error.");
                    ee.printStackTrace();
                }
            }
        }

        /**
         *   View semantics structure of pipeline.
         *
         * @param t Http context.
         * @throws IOException
         */
        public void handleGet(HttpExchange t) throws IOException
        {
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();
            inferer.displayStructure(out);
            out.close();
        }

        /**
         *   Handle other unimplemented methods.
         *
         * @param t
         * @throws IOException
         */
        private void handleOtherMethods(HttpExchange t) throws IOException
        {
            String info = "Method Not Implemented!\n";
            t.sendResponseHeaders(404, info.length());
            OutputStream out = t.getResponseBody();
            out.write(info.getBytes());
            out.close();
        }
    }

    private String host;
    private int port;
    private String data_server_location;
    private ESSlave es_slave;
    private SemanticsInferer inferer;
    private Scorer scorer;
    private PrintWriter log_file;
    private HashMap<String, Tunnel> tun_pool;
    //private HashMap<String, LogLine> log_pool;

    /**
     *   Create an instance with default configuration.
     *
     * @throws IOException
     */
    public PipeServer() throws IOException
    {
        host = ProvConfig.DEF_PS_HOST;
        port = ProvConfig.DEF_PS_PORT;
        data_server_location = ProvConfig.DEF_DS_PROTOCOL + "://" + ProvConfig.DEF_DS_HOST + ':'
                + Integer.toString(ProvConfig.DEF_DS_PORT) + '/';
        es_slave = new ESSlave();
        inferer = new SemanticsInferer();
        scorer = new Scorer();
        tun_pool = new HashMap<String, Tunnel>();
        //log_pool = new HashMap<String, LogLine>();
    }

    /**
     *   Create an instance with user specified configuration.
     *
     * @param host  Host for pipeline server.
     * @param port  Port for pipeline server.
     * @param ds_host Host of data server.
     * @param ds_port Port of data server.
     * @param ess_host Host of ES server.
     * @param ess_port Port of ES server.
     * @throws IOException
     */
    public PipeServer(String host, int port, String ds_host, int ds_port, String ess_host, int ess_port) throws IOException
    {
        this.host = host;
        this.port = port;
        data_server_location = ProvConfig.DEF_DS_PROTOCOL + "://" + ds_host + ':'
                + Integer.toString(ds_port) + '/';
        es_slave = new ESSlave(ess_host, ess_port, ProvConfig.DEF_ESS_INDEX, ProvConfig.DEF_ESS_TYPE);
        inferer = new SemanticsInferer();
        scorer = new Scorer();
        tun_pool = new HashMap<String, Tunnel>();
        //log_pool = new HashMap<String, LogLine>();
    }

    /**
     *   Initiate pipeline server.
     *
     * @throws IOException
     */
    public void initiate() throws IOException
    {
        System.out.println("Pipeline Server Initiating...");
        String log_path = ProvConfig.DEF_PS_LOG_FILE;
        log_file = new PrintWriter(log_path);
        System.out.println("\tWill interact with data server at: " + data_server_location);
        System.out.println("\tWill interact with ES server at: " + es_slave.getESSLocation());
        System.out.printf("\tLogs In: '%s'\n", log_path);
        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
        HttpContext data_context = server.createContext(ProvConfig.DEF_PS_DATA_CONTEXT, new DataHandler());
        HttpContext sem_context = server.createContext(ProvConfig.DEF_PS_SEM_CONTEXT, new SemanticsHandler());

        es_slave.rebuild();

        server.setExecutor(null);
        server.start();
        System.out.printf("Pipeline Server Initiated. Listening on=> %s:%d\n", host, port);
    }
}
