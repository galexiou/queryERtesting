/*
 *    This program is free software; you can redistribute it and/or modify
 *    it under the terms of the GNU General Public License as published by
 *    the Free Software Foundation; either version 2 of the License, or
 *    (at your option) any later version.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU General Public License for more details.
 *
 *    Copyright (C) 2015 George Antony Papadakis (gpapadis@yahoo.gr)
 */

package org.imsi.queryEREngine.imsi.er.MetaBlocking;


import org.imsi.queryEREngine.imsi.er.Comparators.ComparisonWeightComparator;
import org.imsi.queryEREngine.imsi.er.DataStructures.AbstractBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.BilateralBlock;
import org.imsi.queryEREngine.imsi.er.DataStructures.Comparison;
import org.imsi.queryEREngine.imsi.er.Utilities.QueryComparisonIterator;

import java.util.*;


public class CardinalityEdgePruning extends AbstractMetablocking {

    protected long kThreshold;
    protected double minimumWeight;
    protected Queue<Comparison> topKEdges;
    protected Set<Integer> qIds;
    protected int[][] entityBlocks;
    protected double averageWeight = Double.MIN_VALUE;
    protected double selectivity;

    public CardinalityEdgePruning(WeightingScheme scheme) {
        super("Cardinality Edge Pruning (Top-K Edges)", scheme);
    }

    public CardinalityEdgePruning(String description, WeightingScheme scheme) {
        super(description, scheme);
    }

    public CardinalityEdgePruning(WeightingScheme scheme, Set<Integer> qIds, double selectivity) {
        super("", scheme);
        this.qIds = qIds;
        this.selectivity = selectivity;
    }

    //    private void addComparison(Comparison comparison, HashMap<Double,Integer> levels) {
    private void addComparison(Comparison comparison) {
        topKEdges.add(comparison);
        if (kThreshold < topKEdges.size()) {
            Comparison lastComparison = topKEdges.poll();
            minimumWeight = lastComparison.getUtilityMeasure();
//            levels.putIfAbsent(minimumWeight,0);
//            levels.put(minimumWeight, levels.get(minimumWeight) + 1);
//            levels.computeIfPresent(minimumWeight,(key, val) -> val+= val++);

        }
    }

    @Override
    public void applyProcessing(List<AbstractBlock> blocks) {
        double s = System.currentTimeMillis();
        initializeEntityIndex(blocks);
//        System.out.println("Statistics: " + (System.currentTimeMillis() - s) / 1000);
        getKThreshold(blocks);
        filterComparisons(blocks);
        gatherComparisons(blocks);
    }

    protected void removeIf(Comparison c) {
        if (c.getUtilityMeasure() < averageWeight) topKEdges.remove(c);
    }

    protected void filterComparisons(List<AbstractBlock> blocks) {
        minimumWeight = Double.MIN_VALUE;
        topKEdges = new PriorityQueue<Comparison>((int) ( 2*kThreshold), new ComparisonWeightComparator());

        int ccounter = 0;
        int counterSelf = 0;
        int limit = (int) Math.floor(qIds.size());
        System.out.println(limit);
        double mean = 0.0f;
        int counter = 0;

        for (AbstractBlock block : blocks) {

            HashSet<Comparison> uComp = new HashSet<>();
            QueryComparisonIterator iterator = block.getQueryComparisonIterator(qIds);
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();
                int entity1 = comparison.getEntityId1();
                int entity2 = comparison.getEntityId2();

                if (entity1 == entity2) continue;

                if (entity1 > entity2)
                    comparison = new Comparison(false, entity2, entity1);

                if (uComp.contains(comparison)) continue;
                double weight = getWeight(block.getBlockIndex(), comparison);
                uComp.add(comparison);
                if (weight < 0 || weight < averageWeight || weight < minimumWeight) {
                    continue;
                }
                comparison.setUtilityMeasure(weight);
                addComparison(comparison);
                if (counter < limit) mean += weight;
                else if (counter == limit) {
                    averageWeight = mean / counter;
                    while (topKEdges.poll().getUtilityMeasure() < averageWeight) ;
                }
                counter++;
            }
        }
    }

    private void gatherComparisons(List<AbstractBlock> blocks) {
        boolean cleanCleanER = blocks.get(0) instanceof BilateralBlock;
        blocks.clear();
        //System.out.println(topKEdges.peek().getUtilityMeasure());
        blocks.add(getDecomposedBlock(cleanCleanER, topKEdges));
    }

    protected void getKThreshold(List<AbstractBlock> blocks) {
        long blockAssingments = 0;
        long tbc = 0;
        //System.out.println("Blocks size: " + blocks.size());
        for (AbstractBlock block : blocks) {
            blockAssingments += block.getTotalBlockAssignments();
            tbc += block.getNoOfComparisons();
        }
        kThreshold = blockAssingments / 2;
//        kThreshold = (long) (tbc*0.05);
//        kThreshold = (long) (kThreshold * 0.8);
//        System.err.println(Math.sqrt(qIds.size()));
//        System.err.println(Math.sqrt(850000));
//        System.err.println(Math.sqrt(50000));
//        System.err.println(Math.sqrt(50000));s
//        kThreshold = (long) (qIds.size() / 0.75);
    }

    public int getNoOfCommonBlocks(Comparison comparison) {
        int[] blocks1 = entityBlocks[comparison.getEntityId1()];
        int[] blocks2 = entityBlocks[comparison.getEntityId2()];

        int commonBlocks = 0;
        int noOfBlocks1 = blocks1.length;
        int noOfBlocks2 = blocks2.length;
        for (int i = 0; i < noOfBlocks1; i++) {
            for (int j = 0; j < noOfBlocks2; j++) {
                if (blocks2[j] < blocks1[i]) {
                    continue;
                }

                if (blocks1[i] < blocks2[j]) {
                    break;
                }

                if (blocks1[i] == blocks2[j]) {
                    commonBlocks++;
                }
            }
        }

        return commonBlocks;
    }

    Queue removelast(Queue<Comparison> pq) {

        Queue<Comparison> pqnew = new PriorityQueue<Comparison>((int) (2 * kThreshold - 180000), new ComparisonWeightComparator());


        while (pq.size() > (kThreshold - 180000)) {
            pqnew.add((Comparison) pq.poll());
            topKEdges.poll();
        }

        return pqnew;
    }
}
