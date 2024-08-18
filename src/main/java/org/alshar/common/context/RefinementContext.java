package org.alshar.common.context;

import org.alshar.Context;
import org.alshar.common.enums.RefinementAlgorithm;

import java.util.ArrayList;
import java.util.List;

public class RefinementContext {
    public List<RefinementAlgorithm> algorithms = new ArrayList<>();
    public LabelPropagationRefinementContext lp;
    public KwayFMRefinementContext kwayFM;
    public JetRefinementContext jet;
    public MtKaHyParRefinementContext mtkahypar;
}
