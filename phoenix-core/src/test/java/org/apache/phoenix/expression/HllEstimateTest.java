package org.apache.phoenix.expression;

import com.google.common.collect.Lists;
import com.twitter.algebird.HyperLogLog;
import com.twitter.algebird.HyperLogLogMonoid;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.phoenix.expression.function.HllEstimateFunction;
import org.apache.phoenix.schema.PDataType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HllEstimateTest {
    @Test
    public void testHllEstimateExpression() throws Exception {
        byte[] data = new byte[4];
        for (int i = 0; i < 4; i++) data[i] = (byte)3;
        Expression hllLiteral = LiteralExpression.newConstant(
                HyperLogLog.toBytes(new HyperLogLogMonoid(12).create(data)),
                PDataType.VARBINARY);
        Expression hllEstimateExpression = new HllEstimateFunction(Lists.newArrayList(hllLiteral));

        ImmutableBytesWritable ptr = new ImmutableBytesWritable();
        hllEstimateExpression.evaluate(null, ptr);
        Object result = hllEstimateExpression.getDataType().toObject(ptr);

        assertTrue(result instanceof Long);
        long resultLong = (long)result;
        assertEquals(1L, resultLong);
    }
}
