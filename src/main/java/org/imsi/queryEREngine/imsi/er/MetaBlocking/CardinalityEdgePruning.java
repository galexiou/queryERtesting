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
    protected Set<Integer> bIds;
    protected AbstractBlock blocka;
    protected double averageWeight =  Double.MIN_VALUE;

    public CardinalityEdgePruning(WeightingScheme scheme) {
        super("Cardinality Edge Pruning (Top-K Edges)", scheme);
    }

    public CardinalityEdgePruning(String description, WeightingScheme scheme) {
        super(description, scheme);
    }

    public CardinalityEdgePruning(WeightingScheme scheme, Set<Integer> qIds) {
        super("", scheme);
        this.qIds = qIds;

    }

    public CardinalityEdgePruning(WeightingScheme scheme, Set<Integer> qIds, int[][] entityBlocks) {
        super("", scheme);
        this.qIds = qIds;
        this.entityBlocks = entityBlocks;
    }

    public CardinalityEdgePruning(WeightingScheme scheme, Set<Integer> qIds, int[][] entityBlocks, AbstractBlock blocka) {
        super("", scheme);
        this.qIds = qIds;
        this.entityBlocks = entityBlocks;
        this.blocka = blocka;
    }


    //    private void addComparison(Comparison comparison, HashMap<Double,Integer> levels) {
    private void addComparison(Comparison comparison) {
        if (comparison.getUtilityMeasure() < minimumWeight) {
            return;
        }

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
//        getStatistics(blocks);
        initializeEntityIndex(blocks);
//        this.entityBlocks = getEntityIndex().getEntityBlocks();
        System.out.println("Statistics: " + (System.currentTimeMillis() - s) / 1000);
        getKThreshold(blocks);
//        bIds = new HashSet<>();
//        for (AbstractBlock b : blocks) {
//            bIds.add(b.getBlockIndex());
//        }
        filterComparisons(blocks);
        gatherComparisons(blocks);
    }

    protected void filterComparisons(List<AbstractBlock> blocks) {
        minimumWeight = Double.MIN_VALUE;
        topKEdges = new PriorityQueue<Comparison>((int) (2 * kThreshold), new ComparisonWeightComparator());

        int wcounter = 0;
        int ccounter = 0;
        int counterSelf = 0;
        int limit = 3000;
        double mean = 0.0f;
        int counter = 0;
//        for (AbstractBlock block : blocks) {
//            HashSet<Comparison> uComp = new HashSet<>();
//            QueryComparisonIterator iterator = block.getQueryComparisonIterator(qIds);
//            while (iterator.hasNext()) {
//                Comparison comparison = iterator.next();
//                if (comparison.getEntityId1() == comparison.getEntityId2()) continue;
//
//                if (comparison.getEntityId1() > comparison.getEntityId2())
//                    comparison = new Comparison(false, comparison.getEntityId2(), comparison.getEntityId1());
//
//                if (uComp.contains(comparison)) continue;
//                if (isRepeated(block.getBlockIndex(), comparison)) continue;
//                uComp.add(comparison);
//                double weight = getNoOfCommonBlocks(comparison);
//                if(weight<3) wcounter++;
//                if(weight==3) counter3++;
//                ccounter++;
//                comparison.setUtilityMeasure(weight);
//                addComparison(comparison);
//            }
//        }
        for (AbstractBlock block : blocks) {

//            if(block instanceof UnilateralBlock ){
//                UnilateralBlock uBlock = (UnilateralBlock) block;
//                if(uBlock.getQueryEntities().length == 0) continue;
//            }

            HashSet<Comparison> uComp = new HashSet<>();
            QueryComparisonIterator iterator = block.getQueryComparisonIterator(qIds);
            while (iterator.hasNext()) {
                Comparison comparison = iterator.next();

                ccounter++;


                if (comparison.getEntityId1() == comparison.getEntityId2()) continue;

                counterSelf++;

                if (comparison.getEntityId1() > comparison.getEntityId2())
                    comparison = new Comparison(false, comparison.getEntityId2(), comparison.getEntityId1());

                if (uComp.contains(comparison)) continue;
//                double weight = getWeightWithBlocks(block.getBlockIndex(), comparison,blocks);
//                double weight = getUniWeight(block.getBlockIndex(), comparison,(UnilateralBlock)block);
                double weight = getWeight(block.getBlockIndex(), comparison);

//                double weight = super.getNoOfCommonBlocks(block.getBlockIndex(), comparison);
//                int[] weightAndFCB = getWeightWithFCB(block.getBlockIndex(), comparison);
//                double weight = weightAndFCB[0];
                uComp.add(comparison);
                if (weight < 0 || weight < averageWeight) {
                    continue;
                }
//                if (weight < 0 ) {
//                    continue;
//                }
                if (counter < limit) mean += weight;
                else if (counter == limit) {
                    averageWeight = mean/limit;
                    System.err.println("AVG\t" + averageWeight);
                }
                counter++;
//                if(weight==1) counter3++;
//                if(weight<1) wcounter++;
//                ccounter++;
//                mean+=weight;
//                if(Double.isInfinite(weight)) System.err.println(weight);


                comparison.setUtilityMeasure(weight);
//                addComparison(comparison,lvls);
                addComparison(comparison);
//                if(ccounter==1) minimumWeight=weight+1;
            }
        }

//        for(Double d : lvls.keySet()){
//           if(d>20.0) System.out.println(d+" : "+lvls.get(d));
//        }
//        System.err.println(wcounter+"   "+ccounter+"  "+counter3);
//        System.err.println(mean / ccounter);
        System.err.println(ccounter + "  " + counterSelf);
    }

    private void gatherComparisons(List<AbstractBlock> blocks) {
        boolean cleanCleanER = blocks.get(0) instanceof BilateralBlock;
        blocks.clear();

//        Queue<Comparison> pqnew = new PriorityQueue<Comparison>((int) (1.5 * kThreshold), new ComparisonWeightComparator());
//        pqnew.addAll(topKEdges);
////        for(Comparison comparison : pqnew){
//        double threshold = topKEdges.poll().getUtilityMeasure();
//        while (threshold==topKEdges.poll().getUtilityMeasure());
//        for(int i=0;i<topKEdges.size();i++){
////            double thres =0.0d;
////            if(i==0)  thres=topKEdges.poll().getUtilityMeasure();
//            double meas = topKEdges.poll().getUtilityMeasure();
//            if(meas>threshold) {
//                System.err.println(meas + "  -> " + i);
//                break;
//            }
////            if(count>100) break;
//        }
        System.out.println(topKEdges.peek().getUtilityMeasure());
//        Queue<Comparison> ll = removelast(topKEdges);
//        System.out.println(ll.peek().getUtilityMeasure());

//        HashSet<Comparison> set = new HashSet<>();
//        set.addAll(topKEdges);
//        System.out.println(set.size()+"   "+ topKEdges.size());
        blocks.add(getDecomposedBlock(cleanCleanER, topKEdges));
    }

    protected void getKThreshold(List<AbstractBlock> blocks) {
        long blockAssingments = 0;
        long tbc = 0;
        System.out.println("Blcokgs size: " + blocks.size());
        for (AbstractBlock block : blocks) {
//            UnilateralBlock bb = (UnilateralBlock) block;
//            bb.setQueryEntities();
            blockAssingments += block.getTotalBlockAssignments();
            tbc += block.getNoOfComparisons();
        }
        System.err.println(blockAssingments + "     " + tbc);
        kThreshold = blockAssingments / 2;
//        kThreshold -= 180000;
        kThreshold = (long) (kThreshold * 0.8);
//        System.err.println(Math.sqrt(qIds.size()));
//        System.err.println(Math.sqrt(850000));
//        System.err.println(Math.sqrt(50000));
//        System.err.println(Math.sqrt(50000));
//        kThreshold = (long) (qIds.size() / 0.75);
        System.out.println(kThreshold);
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
//            pq.poll();
        }

//        pq.clear();
        return pqnew;
//        return pq;
    }
}
