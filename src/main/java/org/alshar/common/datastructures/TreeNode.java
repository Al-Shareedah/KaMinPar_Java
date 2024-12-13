package org.alshar.common.datastructures;

import java.util.ArrayList;
import java.util.List;

public class TreeNode {
    private long label; // Total sum of values
    private List<TreeNode> children; // Child nodes

    public TreeNode(long label) {
        this.label = label;
        this.children = new ArrayList<>();
    }

    public long getLabel() {
        return label;
    }

    public void setLabel(long label) {
        this.label = label;
    }

    public List<TreeNode> getChildren() {
        return children;
    }

    public void addChild(TreeNode child) {
        this.children.add(child);
    }

    @Override
    public String toString() {
        return "TreeNode{" +
                "label=" + label +
                ", children=" + children +
                '}';
    }
}
