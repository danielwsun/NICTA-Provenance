package com.nicta.provenance.pipeline;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author Trams Wang
 * @version 1.0
 * Date: Jan. 22, 2016
 */
public class Scorer {
    public void scorePath(SemanticsInferer.DataNode dst_node, double ans)
    {
        LinkedBlockingQueue<SemanticsInferer.DataNode> q = new LinkedBlockingQueue<SemanticsInferer.DataNode>();
        q.add(dst_node);
        SemanticsInferer.DataNode head = null;
        while (null != (head = q.poll()))
        {
            if (null != head.producer)
            {
                naiveELOMethod(head.producer, ans);
                for (SemanticsInferer.DataNode dn : head.producer.preds)
                {
                    if (!q.contains(dn))
                    {
                        q.add(dn);
                    }
                }
            }
        }
    }

    private void naiveELOMethod(SemanticsInferer.ProcessNode node, double ans)
    {
        node.score += ans;
    }
}
