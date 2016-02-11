package com.nicta.provenance.query;

import com.nicta.provenance.ProvConfig;
import com.sun.net.httpserver.HttpContext;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;

/**
 * Created by babyfish on 16-2-1.
 */
public class QueryServer {
    /*private class DataHandler implements HttpHandler{
        ;
    }

    private class SemanticsHandler implements HttpHandler{
        ;
    }

    private class MetaHandler implements HttpHandler{
        ;
    }

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
                try
                {
                    String info = "Error In Query Server";
                    t.sendResponseHeaders(400, info.length());
                    OutputStream out = t.getResponseBody();
                    out.write(info.getBytes());
                    out.close();
                }
                catch (IOException ee)
                {
                    System.out.println("Query Server Nested Error.");
                    ee.printStackTrace();
                }
            }
        }

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
    private String ess_location;

    public QueryServer()
    {
        ps_location = ProvConfig.DEF_PS_PROTOCOL + "://" + ProvConfig.DEF_PS_HOST + ':'
                + Integer.toString(ProvConfig.DEF_PS_PORT) + '/';
        ds_location = ProvConfig.DEF_DS_PROTOCOL + "://" + ProvConfig.DEF_DS_HOST + ':'
                + Integer.toString(ProvConfig.DEF_DS_PORT) + '/';
        ess_location = ProvConfig.DEF_ESS_PROTOCOL + "://" + ProvConfig.DEF_ESS_HOST + ':'
                + Integer.toString(ProvConfig.DEF_ESS_PORT) + '/';
    }

    public void initiate() throws IOException
    {
        System.out.println("QueryServer Initiating...");
        System.out.println("\tCommunicate with PipeServer at: " + ps_location);
        System.out.println("\tCommunicate with DataServer at: " + ds_location);
        System.out.println("\tCommunicate with ESServer at: " + ess_location);
        HttpServer server = HttpServer.create(new InetSocketAddress(ProvConfig.DEF_QS_HOST, ProvConfig.DEF_QS_PORT), 0);
        HttpContext data_context = server.createContext(ProvConfig.DEF_QS_DATA_CONTEXT, new DataHandler());
        HttpContext sem_context = server.createContext(ProvConfig.DEF_QS_SEM_CONTEXT, new SemanticsHandler());
        HttpContext meta_context = server.createContext(ProvConfig.DEF_QS_META_CONTEXT, new MetaHandler());
        HttpContext help_context = server.createContext(ProvConfig.DEF_QS_HELP_CONTEXT, new HelpHandler());
        server.setExecutor(null);
        server.start();
        System.out.println("QueryServer Initiated");
    }*/
}
