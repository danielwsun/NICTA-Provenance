package com.nicta.provenance.pipeline;

/**
 * @author Trams Wang
 * @version 1.2
 * Date: Jan. 19, 2016
 *
 *   Representing the log line information that will be stored into ES server.
 */
public class LogLine {
    public String srcvar;
    public String srcidx;
    public String processor;
    public String dstvar;
    public String dstidx;
    public LogLine()
    {
        srcvar = null;
        srcidx = null;
        processor = null;
        dstvar = null;
        dstidx = null;
    }
}
