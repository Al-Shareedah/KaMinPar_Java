package org.alshar.kaminpar_shm.initialPartitioning;
import org.alshar.Graph;
import org.alshar.common.context.InitialRefinementContext;
import org.alshar.common.context.PartitionContext;

import java.util.Optional;
public class InitialRefinerFactory {
    public static InitialRefiner createInitialRefiner(
            Graph graph,
            PartitionContext pCtx,
            InitialRefinementContext rCtx,
            InitialRefiner.MemoryContext mCtx
    ) {
        if (!rCtx.disabled) {
            AdaptiveStoppingPolicy stoppingPolicy = new AdaptiveStoppingPolicy();
            switch (rCtx.stoppingRule) {
                case ADAPTIVE:
                    return new InitialTwoWayFMRefiner<>(
                            graph.n().value, pCtx, rCtx, mCtx, stoppingPolicy
                    );
                case SIMPLE:
                    return new InitialTwoWayFMRefiner<>(
                            graph.n().value, pCtx, rCtx, mCtx, stoppingPolicy
                    );
            }
        }
        return new InitialNoopRefiner(mCtx);
    }
}
