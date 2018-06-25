package org.apache.ignite.internal.processors.blockchain;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;

public class IgniteBlockchainProcessor extends GridProcessorAdapter {
    /**
     * @param ctx Kernal context.
     */
    protected IgniteBlockchainProcessor(GridKernalContext ctx) {
        super(ctx);
    }
}
