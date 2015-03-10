package org.apache.phoenix.expression.function;

import java.sql.SQLException;
import java.util.List;

import com.twitter.algebird.HyperLogLog;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.PDataType;
import org.apache.phoenix.schema.tuple.Tuple;

/**
 * Utility function to get the cardinality estimate from the binary HyperLogLog data.
 */
@BuiltInFunction(name = HllEstimateFunction.NAME,
        args = {
                @Argument(allowedTypes={PDataType.VARBINARY})
        }
)
public class HllEstimateFunction extends ScalarFunction {
    public static final String NAME = "HLL_LONG";

    public HllEstimateFunction() {
    }

    public HllEstimateFunction(List<Expression> children) throws SQLException {
        super(children);
    }

    @Override
    public boolean evaluate(Tuple tuple, ImmutableBytesWritable ptr) {
        if (!getChildExpression().evaluate(tuple, ptr)) {
            return false;
        }

        byte[] source = new byte[ptr.getLength()];
        System.arraycopy(ptr.get(), ptr.getOffset(), source, 0, ptr.getLength());

        ptr.set(PDataType.LONG.toBytes(HyperLogLog.fromBytes(source).approximateSize().estimate()));
        return true;
    }

    @Override
    public PDataType getDataType() {
        return PDataType.LONG;
    }

    @Override
    public boolean isNullable() {
        return getChildExpression().isNullable();
    }

    @Override
    public String getName() {
        return NAME;
    }

    private Expression getChildExpression() {
        return children.get(0);
    }
}
