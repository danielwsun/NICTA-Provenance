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
import java.util.Scanner;

/**
 * @author Trams Wang
 * @version 1.2
 * Date: Jan. 27, 2016
 *
 *   Standalone server for semantics inferring, process unit scoring and data dispatching. It sends result/intermediate
 * data to data server and send log line records to ES server. At mean time, it infers and stores pipeline semantics
 * locally, score all pipeline paths after storing the final result.
 *
 * Protocol:
 *   Context: /
 *     GET: Fetch data from data server.
 *     PUT: Store intermediate results and infer pipeline structure.
 *     POST: Store final results, complete pipeline structure and score the path.
 *   Context: /semantics
 *     GET: View pipeline semantics structure.
 *
 * @see DataHandler for supporting RESTful API.
 */
public class PipeServer {

    /**
     * @author Trams Wang
     * @version 1.1
     * Date: Jan. 19, 2016
     *
     *   This class is used by PipeServer to communicate with Elastic Search Server.
     */
    private class ESSlave{
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
        private String simpleHttpRequest(String str_url, String method, String content) throws IOException
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
            String resp = in.readLine();
            return "ES Resp: " + Integer.toString(resp_code) + ", " + resp;
        }
    }

    /**
     * @author Trams Wang
     * @version  1.3
     * Date Jan. 27, 2016
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
         *   'GET' method handler.
         *   Handle 'GET' request URI of the following form: /?log=encoded_json_string, which fetches from data server
         * the content of 'dstidx'. Note: LogLine.dstidx here denotes the index that refers to the source data.
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

            /* Connect to Data Server*/
            URL url = new URL(data_server_location + log.dstidx);
            HttpURLConnection con = (HttpURLConnection)url.openConnection();
            con.setRequestMethod("GET");
            con.setDoInput(true);
            con.setDoOutput(false);
            int rep_code = t.getResponseCode();
            if (400 == rep_code)
            {
                throw new IOException("Data Server Error.");
            }
            BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()));

            /* Passing data from DS to client*/
            String inputline;
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();
            while (null != (inputline = in.readLine()))
            {
                out.write((inputline + '\n').getBytes());
            }
            out.close();

            /* Store log info into ES server*/
            es_slave.send(log_json);
            inferer.absorb(log.srcvar, log.processor, log.dstvar);
        }

        /**
         *   'PUT' method handler.
         *   Handle 'PUT' request URI of the following form: /?log=encoded_json_string, which receives one json parameter
         * converted from class LogLine where there are some data members already assigned: srcvar, srcidx, processor
         * and dstvar. And then pipeline server will pass data and log info to data server and ES server respectively.
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

            /* Pass data to DS*/
            URL url = new URL(data_server_location + log.dstvar);
            HttpURLConnection ds_con = (HttpURLConnection)url.openConnection();
            ds_con.setRequestMethod("PUT");
            ds_con.setDoOutput(true);
            ds_con.setDoInput(true);
            OutputStream ds_out = ds_con.getOutputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
            String inputline;
            while (null != (inputline = in.readLine()))
            {
                ds_out.write((inputline + '\n').getBytes());
            }
            ds_out.close();
            in.close();

            /* Pass destination index to client*/
            int resp_code = ds_con.getResponseCode();
            if (400 == resp_code)
            {
                throw new IOException("Data Server Error.");
            }
            BufferedReader ds_in = new BufferedReader(new InputStreamReader(ds_con.getInputStream()));
            String dstidx = ds_in.readLine();
            t.sendResponseHeaders(200, dstidx.getBytes().length);
            OutputStream out = t.getResponseBody();
            out.write(dstidx.getBytes());
            out.close();
            ds_in.close();

            /* Store log info into ES server*/
            log.dstidx = dstidx;
            log_json = gson.toJson(log);
            es_slave.send(log_json);
            inferer.absorb(log.srcvar, log.processor, log.dstvar);
        }

        /**
         *   'POST' method handler.
         *   Same protocol with 'PUT' method. Yet it only called by final storing functions instead of intermediate
         * stores. It will ask user whether the data is good or not in quality and score the processing unit according
         * to that answer.
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

            /* Pass data to DS and Answer testing server*/
            URL url = new URL(data_server_location + log.dstvar);
            HttpURLConnection ds_con = (HttpURLConnection)url.openConnection();
            ds_con.setRequestMethod("PUT");
            ds_con.setDoOutput(true);
            ds_con.setDoInput(true);
            OutputStream ds_out = ds_con.getOutputStream();
            BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
            String inputline;

            URL chkurl = new URL("http://localhost:6666/");
            HttpURLConnection chk_con = (HttpURLConnection)chkurl.openConnection();
            chk_con.setRequestMethod("GET");
            chk_con.setDoOutput(true);
            chk_con.setDoInput(true);
            OutputStream chk_out = chk_con.getOutputStream();

            while (null != (inputline = in.readLine()))
            {
                ds_out.write((inputline + '\n').getBytes());
                chk_out.write((inputline + '\n').getBytes());
            }
            ds_out.close();
            chk_out.close();
            in.close();

            /* Pass destination index to client*/
            int resp_code = ds_con.getResponseCode();
            if (400 == resp_code)
            {
                throw new IOException("Data Server Error.");
            }
            BufferedReader ds_in = new BufferedReader(new InputStreamReader(ds_con.getInputStream()));
            String dstidx = ds_in.readLine();
            t.sendResponseHeaders(200, dstidx.getBytes().length);
            OutputStream out = t.getResponseBody();
            out.write(dstidx.getBytes());
            out.close();
            ds_in.close();

            /* Store log info into ES server*/
            log.dstidx = dstidx;
            log_json = gson.toJson(log);
            es_slave.send(log_json);
            inferer.absorb(log.srcvar, log.processor, log.dstvar);

            /* Score data according to user's specification*/
            System.out.println("Checking for similarity...");
            resp_code = chk_con.getResponseCode();
            if (400 == resp_code)
            {
                throw new IOException("Answer Testing Server Error.");
            }
            Scanner scanner = new Scanner(chk_con.getInputStream());
            double ans = scanner.nextDouble();
            System.out.println("Similarity is: " + Double.toString(ans));
            scorer.scorePath(inferer.dataNode(log.srcvar), ans);
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
        System.out.println("\tWill interact with ES server at: " + es_slave.ess_location);
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