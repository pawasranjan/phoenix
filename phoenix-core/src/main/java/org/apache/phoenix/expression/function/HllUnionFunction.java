package org.apache.phoenix.expression.function;

import java.util.List;

import org.apache.hadoop.conf.Configuration;

import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.aggregator.Aggregator;
import org.apache.phoenix.expression.aggregator.HllUnionAggregator;
import org.apache.phoenix.parse.HllUnionParseNode;
import org.apache.phoenix.parse.FunctionParseNode.Argument;
import org.apache.phoenix.parse.FunctionParseNode.BuiltInFunction;
import org.apache.phoenix.schema.PDataType;

/**
 * HLL_UNION function for HyperLogLog.
 * The result is an HLL byte array for the union of the input HLL sketches.
 */
@BuiltInFunction(name = HllUnionFunction.NAME,
        nodeClass = HllUnionParseNode.class,
        args = {
                @Argument(allowedTypes={PDataType.VARBINARY})
        }
)
public class HllUnionFunction extends SingleAggregateFunction {
    static final String NAME = "HLL_UNION";

    public HllUnionFunction() {}

    public HllUnionFunction(List<Expression> children) {
        super(children);
    }

    @Override
    public Aggregator newServerAggregator(Configuration conf) {
        return new HllUnionAggregator();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
