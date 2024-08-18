package org.alshar.common.context;

import org.alshar.common.enums.InitialPartitioningMode;
import org.alshar.common.enums.PartitioningMode;

public class PartitioningContext {
    public PartitioningMode mode;
    public InitialPartitioningMode deepInitialPartitioningMode;
    public double deepInitialPartitioningLoad;
}
