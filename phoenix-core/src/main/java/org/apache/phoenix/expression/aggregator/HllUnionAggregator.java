package org.apache.phoenix.expression.aggregator;

import com.twitter.algebird.HLL;
import com.twitter.algebird.HyperLogLog;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.SortOrder;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Custom aggregator to union HyperLogLog data (VARBINARY -> VARBINARY).
 * Can union HLL sketches of different sizes, but the error in the result will
 * correspond to error for the smallest size HLL in the union.
 */
public class HllUnionAggregator extends BaseAggregator {
    public HllUnionAggregator() {
        super(SortOrder.getDefault());
    }

    private HLL aggHLL = null;

    @Override
    public void aggregate(Tuple tuple, ImmutableBytesWritable ptr) {
        byte[] buffer = new byte[ptr.getLength()];
        System.arraycopy(ptr.get(), ptr.getOffset(), buffer, 0, ptr.getLength());
        HLL thisHLL = HyperLogLog.fromBytes(buffer);
        if (aggHLL == null) {
            aggHLL = thisHLL;
        } else {
            int aggBits = aggHLL.bits();
            int thisBits = thisHLL.bits();
            if (thisBits > aggBits) {
                thisHLL = thisHLL.downsize(aggBits);
            } else if (thisBits < aggBits) {
                aggHLL = aggHLL.downsize(thisBits);
            }
            aggHLL = aggHLL.$plus(thisHLL);
        }

    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (aggHLL == null) {
            ptr.set(PDataType.NULL_BYTES);
        } else {
            ptr.set(HyperLogLog.toBytes(aggHLL));
        }
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.VARBINARY;
    }

    @Override
    public String toString() {
        return "UNION [union=" + aggHLL.approximateSize().estimate() + "]";
    }

    @Override
    public void reset() {
        aggHLL = null;
        super.reset();
    }
}
