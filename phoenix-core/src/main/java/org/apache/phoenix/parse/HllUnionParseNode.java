package org.apache.phoenix.parse;

import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.compile.StatementContext;
import org.apache.phoenix.expression.Expression;
import org.apache.phoenix.expression.function.HllUnionFunction;
import org.apache.phoenix.expression.function.FunctionExpression;

public class HllUnionParseNode extends AggregateFunctionParseNode {
    public HllUnionParseNode(String name, List<ParseNode> children, BuiltInFunctionInfo info) {
        super(name, children, info);
    }

    @Override
    public FunctionExpression create(List<Expression> children, StatementContext context) throws SQLException {
        return new HllUnionFunction(children);
    }
}
