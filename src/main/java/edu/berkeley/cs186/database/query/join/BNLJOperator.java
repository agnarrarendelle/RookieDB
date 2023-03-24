package edu.berkeley.cs186.database.query.join;

import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.iterator.BacktrackingIterator;
import edu.berkeley.cs186.database.query.JoinOperator;
import edu.berkeley.cs186.database.query.QueryOperator;
import edu.berkeley.cs186.database.table.Record;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Performs an equijoin between two relations on leftColumnName and
 * rightColumnName respectively using the Block Nested Loop Join algorithm.
 */
public class BNLJOperator extends JoinOperator {
    protected int numBuffers;

    public BNLJOperator(QueryOperator leftSource,
                        QueryOperator rightSource,
                        String leftColumnName,
                        String rightColumnName,
                        TransactionContext transaction) {
        super(leftSource, materialize(rightSource, transaction),
                leftColumnName, rightColumnName, transaction, JoinType.BNLJ
        );
        this.numBuffers = transaction.getWorkMemSize();
        this.stats = this.estimateStats();
    }

    @Override
    public Iterator<Record> iterator() {
        return new BNLJIterator();
    }

    @Override
    public int estimateIOCost() {
        //This method implements the IO cost estimation of the Block Nested Loop Join
        int usableBuffers = numBuffers - 2;
        int numLeftPages = getLeftSource().estimateStats().getNumPages();
        int numRightPages = getRightSource().estimateIOCost();
        return ((int) Math.ceil((double) numLeftPages / (double) usableBuffers)) * numRightPages +
               getLeftSource().estimateIOCost();
    }

    /**
     * A record iterator that executes the logic for a simple nested loop join.
     * Look over the implementation in SNLJOperator if you want to get a feel
     * for the fetchNextRecord() logic.
     */
    private class BNLJIterator implements Iterator<Record>{
        // Iterator over all the records of the left source
        //used to fetch next block from left source
        private Iterator<Record> leftSourceIterator;
        // Iterator over all the records of the right source
        private BacktrackingIterator<Record> rightSourceIterator;
        // Iterator over records in the current block of left pages
        // used to fetch next record in the current left block
        private BacktrackingIterator<Record> leftBlockIterator;
        // Iterator over records in the current right page
        private BacktrackingIterator<Record> rightPageIterator;
        // The current record from the left relation
        private Record leftRecord;
        // The next record to return
        private Record nextRecord;

        private BNLJIterator() {
            super();
            this.leftSourceIterator = getLeftSource().iterator();
            this.fetchNextLeftBlock();

            this.rightSourceIterator = getRightSource().backtrackingIterator();
            this.rightSourceIterator.markNext();
            this.fetchNextRightPage();

            this.nextRecord = null;
        }

        /**
         * Fetch the next block of records from the left source.
         * leftBlockIterator should be set to a backtracking iterator over up to
         * B-2 pages of records from the left source, and leftRecord should be
         * set to the first record in this block.
         *
         * If there are no more records in the left source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextLeftBlock() {
            // TODO(proj3_part1): implement
            //If the left source has not more records
            //simply return and do nothing
            if(!this.leftSourceIterator.hasNext())
                return;

            //create a block-sized( (numOfBuffers - 2) pages) chunk of records,
            //and return an iterator to it
            this.leftBlockIterator = QueryOperator.getBlockIterator(this.leftSourceIterator, getLeftSource().getSchema(), numBuffers - 2);
            //mark the first record in the block so that it can be used later
            this.leftBlockIterator.markNext();
            //set leftRecord to the first record in the block
            this.leftRecord = this.leftBlockIterator.next();
        }

        /**
         * Fetch the next page of records from the right source.
         * rightPageIterator should be set to a backtracking iterator over up to
         * one page of records from the right source.
         *
         * If there are no more records in the right source, this method should
         * do nothing.
         *
         * You may find QueryOperator#getBlockIterator useful here.
         */
        private void fetchNextRightPage() {
            // TODO(proj3_part1): implement
            //If the right source has not more records
            //simply return and do nothing
            if(!this.rightSourceIterator.hasNext())
                return;

            //fetch the next page of records from right source and return an iterator to it
            this.rightPageIterator = QueryOperator.getBlockIterator(this.rightSourceIterator, getRightSource().getSchema(),1);
            //mark the first record in the page so that it can be used later
            this.rightPageIterator.markNext();
        }

        /**
         * Returns the next record that should be yielded from this join,
         * or null if there are no more records to join.
         *
         * You may find JoinOperator#compare useful here. (You can call compare
         * function directly from this file, since BNLJOperator is a subclass
         * of JoinOperator).
         */
        private Record fetchNextRecord() {
            // TODO(proj3_part1): implement
            while (true) {
                //predicate:none
                //case 1:the current right page still has record left
                if (this.rightPageIterator.hasNext()) {
                //predicate:
                //1. current right page has no record left
                //case 2:the current left block still has record left
                } else if (this.leftBlockIterator.hasNext()) {
                    this.leftRecord = this.leftBlockIterator.next();
                    this.rightPageIterator.reset();
                //predicate:
                //1. current right page has no record left
                //2. current left block has no record left
                //case 3:the right source still has pages left
                } else if (this.rightSourceIterator.hasNext()) {
                    this.leftBlockIterator.reset();
                    this.leftRecord = this.leftBlockIterator.next();
                    fetchNextRightPage();
                //predicate:
                //1. current right page has no record left
                //2. current left block still has no record left
                //3. the right source has no more pages
                //case 4:the right source still has page left
                } else if (this.leftSourceIterator.hasNext()) {
                    fetchNextLeftBlock();
                    this.rightSourceIterator.reset();
                    fetchNextRightPage();
                } else {
                    return null;
                }

                //get next right record
                Record rightRecord = rightPageIterator.next();
                //if the next right record matches the current left record
                //join them and return the result
                if (compare(this.leftRecord, rightRecord) == 0) {
                    return this.leftRecord.concat(rightRecord);
                }
            }
        }

        /**
         * @return true if this iterator has another record to yield, otherwise
         * false
         */
        @Override
        public boolean hasNext() {
            if (this.nextRecord == null) this.nextRecord = fetchNextRecord();
            return this.nextRecord != null;
        }

        /**
         * @return the next record from this iterator
         * @throws NoSuchElementException if there are no more records to yield
         */
        @Override
        public Record next() {
            if (!this.hasNext()) throw new NoSuchElementException();
            Record nextRecord = this.nextRecord;
            this.nextRecord = null;
            return nextRecord;
        }
    }
}
