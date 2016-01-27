package test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;

/**
 * Created by babyfish on 16-1-28.
 */
public class AnswerServer {
    static class Handler implements HttpHandler {
    ArrayList<String> ans;

    public Handler()
    {
        try
        {
            ans = new ArrayList<String>();
            BufferedReader in = new BufferedReader(new FileReader("./Data/WifiStatusLocAns.csv"));
            String inputline;
            while (null != (inputline = in.readLine()))
            {
                ans.add(inputline.split(",")[0]);
            }
            in.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.exit(0);
        }
    }
    public void handle(HttpExchange t) throws IOException
    {
        System.out.println("Handle");
        ArrayList<String> result = new ArrayList<String>();
        BufferedReader in = new BufferedReader(new InputStreamReader(t.getRequestBody()));
        String inputline;
        while (null != (inputline = in.readLine()))
        {
            result.add(inputline.split(",")[0]);
        }
        //Compare ans with result
        String rate = Double.toString((double)LCS(ans, result) / ans.size());
        t.sendResponseHeaders(200, rate.getBytes().length);
        OutputStream out = t.getResponseBody();
        out.write(rate.getBytes());
        out.close();
        System.out.println("Rate: "+rate);
    }
}

    public static void main(String args[]) throws IOException
    {
        HttpServer server = HttpServer.create(new InetSocketAddress("localhost", 6666), 0);
        server.createContext("/", new Handler());
        server.setExecutor(null);
        server.start();
        System.out.println("Answer server initiated.");
    }

    static public int LCS(ArrayList<String> la, ArrayList<String> lb)
    {
        int rows = lb.size()+1;
        int cols = la.size()+1;
        int [][] tab = new int[rows][cols];
        for (int i = 0; i < rows; i++)
            tab[i][0] = 0;
        for (int j = 0; j < cols; j++)
            tab[0][j] = 0;

        //Start
        for (int i = 1; i < rows; i++)
        {
            for (int j = 1; j < cols; j++)
            {
                int same = lb.get(i-1).equals(la.get(j-1))?1:0;
                tab[i][j] = Math.max(Math.max(tab[i-1][j], tab[i][j-1]), tab[i-1][j-1] + same);
                //tab[i][j] = (tab[i-1][j] > tab[i][j-1])?tab[i-1][j]:tab[i][j-1];
                //tab[i][j] = (tab[i][j] > (tab[i-1][j-1] + (lb.get(i).equals(la.get(j))?1:0)))?tab[i][j]:(tab[i-1][j-1] + (lb.get(i).equals(la.get(j))?1:0));
            }
        }
        System.out.println("LCS done.");
        return tab[rows-1][cols-1];
    }
}
