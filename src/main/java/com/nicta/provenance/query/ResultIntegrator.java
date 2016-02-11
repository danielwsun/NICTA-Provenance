package com.nicta.provenance.query;

import java.util.Stack;

/**
 * Created by babyfish on 16-2-1.
 */
public class ResultIntegrator {
    /*public static final String PROMPT = "Results: ";
    public static final String LINEEND = "<-**->";

    public void DisplayData(byte[][] results)
    {
        System.out.println(PROMPT + "<DATA>");
        for (int i = 0; i < results.length; i++)
        {
            System.out.printf("%d: %s\n", i, new String(results[i]));
        }
        System.out.println(LINEEND);
    }
    public void DisplayMeta(LogLine[] logs)
    {
        System.out.println(PROMPT + "<META>");
        int no_len = 0;
        int time_len = 0;
        int float_len = 0;
        int proc_len = "Processor".length();
        int src_len = 3;
        int dst_len = 3;
        for (int i = 0; i < logs.length; i++)
        {
            int len = logs[i].time.toString().length();
            if (time_len < len) time_len = len;
            len = logs[i].processor.length();
            if (proc_len < len) proc_len = len;
            len = logs[i].srcidx.length();
            if (src_len < len) src_len = len;
            len = logs[i].dstidx.length();
            if (dst_len < len) dst_len = len;
        }
        no_len = String.valueOf(logs.length).length();
        //no_len = 3;
        float_len = 8;
        String fmt = "%"+Integer.toString(no_len)+"s %"+Integer.toString(time_len)+"s %"
                +Integer.toString(float_len)+"s %"+Integer.toString(proc_len)+"s %"
                +Integer.toString(src_len)+"s %"+Integer.toString(dst_len)+"s\n";
        System.out.printf(fmt, "#", "Time", "Duration", "Processor", "Src", "Res");
        fmt = "%"+Integer.toString(no_len)+"d %"+Integer.toString(time_len)+"s %"
                +Integer.toString(float_len)+"d %"+Integer.toString(proc_len)+"s %"
                +Integer.toString(src_len)+"s %"+Integer.toString(dst_len)+"s\n";
        for (int i = 0; i < logs.length; i++)
        {
            System.out.printf(fmt, i, logs[i].time.toString(), (int)logs[i].duration, logs[i].processor, logs[i].srcidx, logs[i].dstidx);
        }
        System.out.println(LINEEND);
    }
    public void DisplaySinglePipeline(String[][] processors)
    {
        System.out.println(PROMPT + "<SINGLE PIPELINE PATH>");
        for (int i = 0; i < processors.length; i++)
        {
            System.out.printf("Pipeline %d:\n", i);
            for (int j = 0; j < processors[i].length - 1; j++)
            {
                System.out.print(processors[i][j] + "->");
                //System.out.println("V");
            }
            System.out.println(processors[i][processors[i].length - 1]);
        }
        System.out.println(LINEEND);
    }
    public void DisplayAllPipeline(TreeNode[] trees)
    {
        System.out.println(PROMPT + "<PIPELINE TREE>");
        for (int i = 0; i < trees.length; i++)
        {
            System.out.printf("Pipeline tree %d:\n", i);
            System.out.print("->");
            Stack<Integer> stack = new Stack<Integer>();
            stack.push(1);
            PrintTree(stack, trees[i]);
        }
        System.out.println(LINEEND);
    }
    private void PrintTree(Stack<Integer> stack, TreeNode root)
    {
        if (null == root) return;
        stack.set(stack.size() - 1, stack.peek() - 1);
        if (null != root.succs)
        {
            stack.push(root.succs.length);
            //Print root itself
            System.out.print(root.proc);
            //Print first successor
            System.out.print("->");
            PrintTree(stack, root.succs[0]);
            //Print rest successors
            for (int i = 1; i < root.succs.length; i++) {
                //System.out.println();
                for (int j = 0; j < stack.size() - 1; j++) {
                    if (0 < stack.get(j)) {
                        System.out.print("|     ");
                    } else {
                        System.out.print("      ");
                    }
                }
                System.out.print("L>");
                PrintTree(stack, root.succs[i]);
            }
            stack.pop();
        }
        else
        {
            //Print root itself
            System.out.println(root.proc);
        }
    }*/
}
