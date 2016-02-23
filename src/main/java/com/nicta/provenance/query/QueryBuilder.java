package com.nicta.provenance.query;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Stack;

/**
 * Created by babyfish on 16-2-1.
 */

/**
 * @author TramsWang
 * @version 2.0
 * Date: Feb. 1, 2016
 *
 *   This class is used for parsing query expressions. It converts SQL-like query expressions into elasticsearch query
 * jsons.
 */
public class QueryBuilder {
    /* DSL operators*/
    public static final String  TV_EOL = "";
    public static final String  TV_AND = "&&";
    public static final String  TV_OR = "||";
    public static final String  TV_NOT = "!";
    public static final String  TV_LP = "(";
    public static final String  TV_RP = ")";
    public static final String  TV_EQ = "==";
    public static final String  TV_GT = ">";
    public static final String  TV_LT = "<";

    public static final int     TT_EOL = 0;
    public static final int     TT_OP_AND = 1;
    public static final int     TT_OP_OR = 2;
    public static final int     TT_OP_NOT = 3;
    public static final int     TT_OP_LP = 4;
    public static final int     TT_OP_RP = 5;
    public static final int     TT_OP_EQ = 6;
    public static final int     TT_OP_GT = 7;
    public static final int     TT_OP_LT = 8;
    public static final int     TT_PARA = 9;

    /**
     * @author TramsWang
     * @version 1.0
     * Date: Feb. 1, 2016
     *
     *   Wrapper class for tokens in query strings.
     */
    private class Token{
        public int type;
        public String val;

        public Token()
        {
            type = TT_EOL;
            val = "";
        }

        public Token(int type, String val)
        {
            this.type = type;
            this.val = val;
        }
    }

    /**
     * @author TramsWang
     * @version 1.0
     * Date: Feb. 1, 2016
     *
     *   Expression node in the syntax tree.
     */
    private class Expression{
        Expression le;
        Token lt;
        Token op;
        Token rt;
        Expression re;

        public Expression()
        {
            le = null;
            lt = null;
            op = null;
            rt = null;
            re = null;
        }

        /**
         *   Convert one syntax tree rooted by this node to a SQL-like expression string.
         *
         * @return Expression string.
         */
        public String toString()
        {
            if (null == op) return "null";
            String result = "";

            switch (op.type)
            {
                case TT_OP_AND:
                case TT_OP_OR:
                    result += (null == le)?"null":le.toString();
                    result += op.val;
                    result += (null == re)?"null":re.toString();
                    break;
                case TT_OP_NOT:
                    result += op.val + ((null == re)?"null":re.toString());
                    break;
                case TT_OP_EQ:
                case TT_OP_GT:
                case TT_OP_LT:
                    result += (null == lt)?"null":lt.val;
                    result += op.val;
                    result += (null == rt)?"null":rt.val;
                    break;
            }
            return result;
        }

        /**
         *   Convert one syntax tree rooted by this node to an elasticsearch query json body.
         *
         * @return Expression json string.
         * @throws IOException
         */
        public String toJSON() throws IOException
        {
            String result = null;
            switch (op.type)
            {
                case TT_OP_AND:
                    result = "{\"bool\":{\"must\":[" + le.toJSON() + ',' + re.toJSON() + "]}}";
                    break;
                case TT_OP_OR:
                    result = "{\"bool\":{\"should\":[" + le.toJSON() + ',' + re.toJSON() + "]}}";
                    break;
                case TT_OP_NOT:
                    result = "{\"bool\":{\"must_not\":[" + le.toJSON() + ',' + re.toJSON() + "]}}";
                    break;
                case TT_OP_EQ:
                    result = "{\"term\":{\"" + lt.val + "\":\"" + rt.val + "\"}}";
                    break;
                case TT_OP_GT:
                    result = "{\"range\":{\"" + lt.val + "\":{\"gt\":" + rt.val + "}}}";
                    break;
                case TT_OP_LT:
                    result = "{\"range\":{\"" + lt.val + "\":{\"lt\":" + rt.val + "}}}";
                    break;
                default:
                    throw new IOException("Bad Expression: Unknown operator: " + op.val + "(type:"
                            + Integer.toString(op.type) + ')');
            }
            return result;
        }
    }

    /**
     * @author TramsWang
     * @version 1.0
     * Date: Feb. 1, 2016
     *
     *   This class parses query string into a series of tokens.
     */
    private class TokenScanner {

        private String buf;
        private int idx;

        private TokenScanner(){}

        /**
         *   Initiate scanner with a query string.
         *
         * @param buf Query string.
         */
        public TokenScanner(String buf)
        {
            this.buf = buf;
            idx = 0;
        }

        /**
         *   Scan for next token in query string.
         *
         * @return Next token in the string.
         */
        public Token nextToken()
        {
            skipBlanks();
            Token t = new Token();
            int len = buf.length();
            for (; idx < len; idx++)
            {
                char c = buf.charAt(idx);
                if (isBlank(c))
                {
                    break;
                }
                else
                {
                    t.val += c;
                }
            }

        /* Determine type*/
            if (TV_EOL.equals(t.val))
            {
                t.type = TT_EOL;
            }
            else if (TV_AND.equals(t.val))
            {
                t.type = TT_OP_AND;
            }
            else if (TV_OR.equals(t.val))
            {
                t.type = TT_OP_OR;
            }
            else if (TV_NOT.equals(t.val))
            {
                t.type = TT_OP_NOT;
            }
            else if (TV_LP.equals(t.val))
            {
                t.type = TT_OP_LP;
            }
            else if (TV_RP.equals(t.val))
            {
                t.type = TT_OP_RP;
            }
            else if (TV_EQ.equals(t.val))
            {
                t.type = TT_OP_EQ;
            }
            else if (TV_LT.equals(t.val))
            {
                t.type = TT_OP_LT;
            }
            else if (TV_GT.equals(t.val))
            {
                t.type = TT_OP_GT;
            }
            else
            {
                t.type = TT_PARA;
            }
            return t;
        }

        /**
         *   Skip blank characters in the input string.
         */
        private void skipBlanks()
        {
            int len = buf.length();
            for (; (idx < len) && isBlank(buf.charAt(idx)); idx++);
        }

        /**
         *   Check whether one character is considered blank.
         *
         * @param c Character
         * @return Whether parameter character is considered blank.
         */
        private boolean isBlank(char c)
        {
            switch (c)
            {
                case ' ':
                case '\t':
                    return true;
                default:
                    return false;
            }
        }
    }

    public QueryBuilder(){}

    /**
     *   Convert SQL-like query expression string into complete elasticsearch query json.
     *
     * @param query SQL-like query expression string.
     * @return Elasticsearch query json string.
     * @throws IOException
     */
    public String build(String query) throws IOException
    {
        Expression root = parseSyntaxTree(query);
        String result = "{\"query\":{\"filtered\":{\"filter\":" + root.toJSON() + "}}}";
        return result;
    }

    /**
     *   Print json in a pretty look.
     *
     * @param json Json string.
     * @param out Output stream.
     * @throws IOException
     */
    public void printPrettyJSON(String json, OutputStream out) throws IOException
    {
        int indent = 0;
        int len = json.length();
        for (int i = 0; i < len; i++)
        {
            char c = json.charAt(i);
            switch (c)
            {
                case '{':
                    out.write('\n');
                    for (int j = 0; j < indent; j++) out.write(' ');
                    out.write("{\n".getBytes());
                    indent += 2;
                    for (int j = 0; j < indent; j++) out.write(' ');
                    break;
                case '}':
                    out.write('\n');
                    indent -= 2;
                    for (int j = 0; j < indent; j++) out.write(' ');
                    out.write("}\n".getBytes());
                    for (int j = 0; j < indent; j++) out.write(' ');
                    break;
                case ',':
                    out.write(",\n".getBytes());
                    for (int j = 0; j < indent; j++) out.write(' ');
                    break;
                default:
                    out.write(c);
                    break;
            }
        }
    }

    /**
     *   Parse SQL-like query expressing string into an abstract syntax tree.
     *
     * @param query SQL-like query expression string.
     * @return Root of the syntax tree.
     * @throws IOException
     */
    private Expression parseSyntaxTree(String query) throws IOException
    {
        TokenScanner scanner = new TokenScanner(query);
        Stack<Object> stack = new Stack<Object>();
        boolean cont = true;
        while (cont)
        {
            Token t = scanner.nextToken();
            switch (t.type)
            {
                case TT_EOL:
                    cont = false;
                    break;
                case TT_OP_AND:
                case TT_OP_OR:
                case TT_OP_NOT:
                case TT_OP_LP:
                case TT_OP_EQ:
                case TT_OP_GT:
                case TT_OP_LT:
                    stack.push(t);
                    break;
                case TT_PARA:
                    stack.push(t);
                    foldPara(stack);
                    break;
                case TT_OP_RP:
                    if (stack.empty()) throw new IOException("Bad Token ')'");
                    if (stack.peek() instanceof Expression)
                    {
                        Expression exp = (Expression)stack.pop();
                        if (stack.empty()) throw new IOException("Missing '(' Before Expression: " + exp.toString());
                        if (stack.peek() instanceof Token)
                        {
                            Token ttt = (Token)stack.pop();
                            if (TT_OP_LP == ttt.type)
                            {
                                stack.push(exp);
                            }
                            else
                            {
                                throw new IOException("Missing '(' Before Expression: " + exp.toString());
                            }
                        }
                        else
                        {
                            throw new IOException("Missing '(' Before Expression: " + exp.toString());
                        }
                    }
                    else
                    {
                        Token tt = (Token)stack.pop();
                        throw new IOException("Bad expression: " + tt.val);
                    }
                    foldExpression(stack);
                    break;
                default:
                    throw new IOException("Bad Token Type: " + Integer.toString(t.type));
            }
        }
        /* Finally check syntax errors.*/
        if (1 < stack.size())
        {
            System.err.println("Syntax error:");
            while (!stack.empty())
            {
                Object o = stack.pop();
                if (o instanceof Token)
                {
                    System.err.println("Unexpected Token: " + ((Token)o).val);
                }
                else
                {
                    System.err.println("Unexpected Expression: " + ((Expression)o).toString());
                }
            }
            throw new IOException("Syntax error.");
        }
        else if (1 == stack.size())
        {
            if (stack.peek() instanceof Expression)
            {
                return (Expression)stack.pop();
            }
            else
            {
                throw new IOException("Bad Expression: " + ((Token)stack.peek()).val);
            }
        }
        else
        {
            return new Expression();
        }
    }

    /**
     *   Reduce expression nodes on the stack.
     *
     * @param stack Expression stack.
     * @throws IOException
     */
    private void foldPara(Stack<Object> stack) throws IOException
    {
        Token l1 = (Token)stack.pop();

        if (stack.empty())
        {
            stack.push(l1);
            return;
        }
        if (stack.peek() instanceof Token)
        {
            Token l2 = (Token)stack.pop();
            switch (l2.type)
            {
                case TT_OP_EQ:
                case TT_OP_GT:
                case TT_OP_LT:
                    if (stack.empty()) throw new IOException("Missing left operand: " + l2.val);
                    if (stack.peek() instanceof Token)
                    {
                        Token l3 = (Token)stack.pop();
                        Expression exp = new Expression();
                        exp.lt = l3;
                        exp.op = l2;
                        exp.rt = l1;
                        stack.push(exp);
                        foldExpression(stack);
                    }
                    else
                    {
                        throw new IOException("Unexpected Token: " + l2.val);
                    }
                    break;
                default:
                    stack.push(l2);
                    stack.push(l1);
                    break;
            }
        }
        else
        {
            throw new IOException("Unexpected Token: " + l1.val);
        }
    }

    /**
     *   Reduce expression nodes on the stack.
     *
     * @param stack Expression stack.
     * @throws IOException
     */
    private void foldExpression(Stack<Object> stack) throws IOException
    {
        Expression rexp  = (Expression)stack.pop();
        if (stack.empty())
        {
            stack.push(rexp);
            return;
        }
        if (stack.peek() instanceof Token)
        {
            Token op = (Token)stack.pop();
            switch (op.type)
            {
                case TT_OP_AND:
                case TT_OP_OR:
                    if (stack.empty()) throw new IOException("Missing left operand: " + op.val);
                    if (stack.peek() instanceof Expression)
                    {
                        Expression lexp = (Expression)stack.pop();
                        Expression ne = new Expression();
                        ne.le = lexp;
                        ne.op = op;
                        ne.re = rexp;
                        stack.push(ne);
                        foldExpression(stack);
                    }
                    else
                    {
                        Token t = (Token)stack.peek();
                        throw new IOException("Unexpected Token: " + t.val);
                    }
                    break;
                case TT_OP_NOT:
                {
                    Expression ne = new Expression();
                    ne.op = op;
                    ne.re = rexp;
                    stack.push(ne);
                    foldExpression(stack);
                    break;
                }
                default:
                    stack.push(op);
                    stack.push(rexp);
                    break;
            }
        }
        else
        {
            throw new IOException("Missing operator before: " + rexp.toString());
        }
    }
}
