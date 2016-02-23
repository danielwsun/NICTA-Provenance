package com.nicta.provenance;

/**
 * @author Trams Wang
 * @version 1.0
 * Date: Jan. 20, 2016
 *
 *   Configuration class, maintains all configurations used in provenance system.
 */
public class ProvConfig {
    public static final String  DEF_DS_PROTOCOL = "http";
    public static final String  DEF_DS_HOST = "localhost";
    public static final int     DEF_DS_PORT = 9999;
    public static final String  DEF_DS_DATA_PATH = "./Data";
    public static final String  DEF_DS_CONTEXT = "/";
    public static final String  DEF_DS_LOG_FILE = "DataServerLog.log";

    public static final String  DEF_PS_PROTOCOL = "http";
    public static final String  DEF_PS_HOST = "localhost";
    public static final int     DEF_PS_PORT = 8888;
    public static final String  DEF_PS_DATA_CONTEXT = "/";
    public static final String  DEF_PS_SEM_CONTEXT = "/_semantics";
    public static final String  DEF_PS_LOG_FILE = "PipeServerLog.log";

    public static final String  DEF_ESS_PROTOCOL = "http";
    public static final String  DEF_ESS_HOST = "localhost";
    public static final int     DEF_ESS_PORT = 9200;
    public static final String  DEF_ESS_INDEX = "provenance";
    public static final String  DEF_ESS_TYPE = "log";
    public static final String  DEF_ESS_MAPPING_FILE = "./Mapping/mapping.json";

    public static final String  DEF_SEMANTIC_PATH = "./Semantics";
    public static final String  DEF_SEMANTIC_FILE_NAME = "def_sem.sem";

    public static final char    TUPLE_BEGIN = '(';
    public static final char    TUPLE_DELI = ',';
    public static final char    TUPLE_END = ')';
    public static final char    BAG_BEGIN = '{';
    public static final char    BAG_DELI = ',';
    public static final char    BAG_END = '}';
    public static final char    MAP_BEGIN = '[';
    public static final char    MAP_DELI = ',';
    public static final char    MAP_MATCHER = '#';
    public static final char    MAP_END = ']';

    public static final String  DEF_QS_PROTOCOL = "http";
    public static final String  DEF_QS_HOST = "localhost";
    public static final int     DEF_QS_PORT = 7777;
    public static final String  DEF_QS_DATA_CONTEXT = "/_data";
    public static final String  DEF_QS_SEM_CONTEXT = "/_semantics";
    public static final String  DEF_QS_META_CONTEXT = "/_meta";
    public static final String  DEF_QS_HELP_CONTEXT = "/_help";


    private static final char SETUP_DS_MANU = 'D';
    private static final char SETUP_DS_AUTO = 'd';
    private static final char SETUP_PS_MANU = 'P';
    private static final char SETUP_PS_AUTO = 'p';
    private static final char SETUP_ESS_MANU = 'E';
    private static final char SETUP_ESS_AUTO = 'e';
    private static final char COMMANDLINE_UI = 'c';
    private static final int SWITCH_LEN = 2;
    private static final char SWITCH_PROMPT = '-';
    private static final int DS_ARGS = 3; //Arguments for HOST, PORT and PATH
    private static final int PS_ARGS = 6;
    private static final int ESS_ARGS = 0;
}
