package com.nicta.provenance.data;

import com.google.gson.Gson;
import com.nicta.provenance.ProvConfig;
import com.nicta.provenance.pipeline.LogLine;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.rmi.server.UID;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Trams Wang
 * @version 2.0
 * Date: Feb, 15, 2016
 *
 *   Standalone server for (intermediate)result storing. In provenance system, It receives
 * data from pipeline server and store the data in local FS.
 *
 * @see com.nicta.provenance.data.DataServer.DataServerHandler for supporting RESTful API.
 */
public class DataServer {

    /**
     * @author Trams Wang
     * @version 2.0
     * Date: Feb. 15, 2016
     *
     *   Handler class for DataServer. Handles ONLY 'GET' and 'PUT' methods.
     */
    private class DataServerHandler implements HttpHandler{
        public DataServerHandler() {}

        /**
         * Request dispatcher.
         *
         * @param t HTTP context.
         */
        public void handle(HttpExchange t)
        {
            String method = t.getRequestMethod();
            try
            {
                if ("GET".equals(method))
                {
                    handleGet(t);
                }
                else if ("PUT".equals(method))
                {
                    handlePut(t);
                }
                else
                {
                    handleOtherMethods(t);
                }
            }
            catch (IOException e)
            {
                e.printStackTrace();
                /*try
                {
                    String info = "Error In Data Server";
                    t.sendResponseHeaders(400, info.length());
                    OutputStream out = t.getResponseBody();
                    out.write(info.getBytes());
                    out.close();
                }
                catch (IOException ee)
                {
                    System.out.println("Data Server Nested Error.");
                    ee.printStackTrace();
                }*/
            }
        }

        /**
         * 'GET' method handler.
         *   Handle 'GET' request URI of the following form: "/data_index", which reads data in file 'data_index'.
         *
         * @param t HTTP context.
         * @throws IOException
         */
        private void handleGet(HttpExchange t) throws IOException
        {
            String srcidx = t.getRequestURI().toString();
            BufferedReader in = new BufferedReader(new FileReader(data_path + srcidx));
            t.sendResponseHeaders(200, 0);
            OutputStream out = t.getResponseBody();
            String inputline;
            while (null != (inputline = in.readLine()))
            {
                out.write((inputline + '\n').getBytes());
            }
            out.close();
            in.close();
            System.out.println("DS:: GET DONE");
        }

        /**
         * 'PUT' method handler.
         *   Handle 'PUT' request URI of the following form: "/variable_name", which generates an unified index
         * and stores data into a file named after "variable_name" and the index, from corresponding variable
         * in Pig Latin script.
         *
         * @param t HTTP context.
         * @throws IOException
         */
        private void handlePut(HttpExchange t) throws IOException
        {
            String var = t.getRequestURI().toString().substring(1);
            String idx = new UID().toString();
            PrintWriter out = new PrintWriter(data_path + '/' + var + '_' + idx);
            String inputline;
            BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
            while (null != (inputline = in.readLine()))
            {
                out.println(inputline);
            }
            t.sendResponseHeaders(200, idx.getBytes().length);
            t.getResponseBody().write(idx.getBytes());
            t.getResponseBody().close();
        }

        /**
         * Handle other unimplemented methods
         *
         * @param t HHTP context.
         * @throws IOException
         */
        private void handleOtherMethods(HttpExchange t) throws IOException
        {
            String info = "Method Not Implemented!";
            t.sendResponseHeaders(404, info.getBytes().length);
            OutputStream out = t.getResponseBody();
            out.write(info.getBytes());
            out.close();
        }
    }

    private String host;
    private int port;
    private String data_path;
    private PrintWriter log_file;

    /**
     * Create an instance using default settings.
     */
    public DataServer()
    {
        host = ProvConfig.DEF_DS_HOST;
        port = ProvConfig.DEF_DS_PORT;
        data_path = ProvConfig.DEF_DS_DATA_PATH;
    }

    /**
     * Create an instance according to user specified settings.
     *
     * @param host server location(ip)
     * @param port port for listening
     * @param data_path path to store data files. If the path does't exist, server will automatically
     *                  create one.
     */
    public DataServer(String host, int port, String data_path)
    {
        this.host = host;
        this.port = port;
        this.data_path = data_path;
    }

    /**
     * Initiate server.
     */
    public void initiate() throws IOException
    {
        /* Detect path for data files, if not exist, create one.*/
        File dir = new File(data_path);
        if (dir.exists())
        {
            System.out.printf("Data path: '%s' already exists.\n", data_path);
        }
        else
        {
            System.out.printf("Data path: '%s' does not exist, create one...\n", data_path);
            if (dir.mkdirs())
            {
                System.out.println("Data path created.");
            }
            else
            {
                System.out.println("Data path creation failed!");
                throw new IOException("Data path creation failed!");
            }
        }
            /* Open the log file.*/
        String log_path = ProvConfig.DEF_DS_LOG_FILE;
        log_file = new PrintWriter(log_path);
        System.out.println("Log file opened.");
            /* Initiate server*/
        HttpServer server = HttpServer.create(new InetSocketAddress(host, port), 0);
        HttpContext context = server.createContext(ProvConfig.DEF_DS_CONTEXT, new DataServerHandler());
        server.setExecutor(null);
        server.start();
        System.out.printf("Data Server Initiated.\n");
        System.out.printf("\tListening on=> %s:%d\n", host, port);
        System.out.printf("\tData Stores In: '%s'\n", data_path);
        System.out.printf("\tLogs In: '%s'\n", log_path);
    }
}
