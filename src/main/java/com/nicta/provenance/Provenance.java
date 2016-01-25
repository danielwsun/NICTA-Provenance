package com.nicta.provenance;

import com.nicta.provenance.data.DataServer;
import com.nicta.provenance.pipeline.PipeServer;

import java.io.IOException;

/**
 * @author Trams Wang
 * @version 1.0
 * Date: Jan. 20, 2016
 *
 *   Main class of provenance system, in charge of commands parsing and server deploying.
 */
public class Provenance {
    public static void main(String[] args)
    {
        try
        {
            DataServer ds = new DataServer();
            ds.initiate();
        }
        catch (IOException e)
        {
            System.out.println("DataServer initiation failed!");
            e.printStackTrace();
        }
        try
        {
            PipeServer ps = new PipeServer();
            ps.initiate();
        }
        catch (IOException e)
        {
            System.out.println("PipeServer initiation failed!");
            e.printStackTrace();
        }
    }
}
