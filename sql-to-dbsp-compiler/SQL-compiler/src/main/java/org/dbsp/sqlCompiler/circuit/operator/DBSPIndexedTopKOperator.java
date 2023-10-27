package org.dbsp.sqlCompiler.circuit.operator;

import org.dbsp.sqlCompiler.compiler.errors.InternalCompilerError;
import org.dbsp.sqlCompiler.compiler.frontend.CalciteObject;
import org.dbsp.sqlCompiler.compiler.visitors.outer.CircuitVisitor;
import org.dbsp.sqlCompiler.ir.expression.DBSPClosureExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPComparatorExpression;
import org.dbsp.sqlCompiler.ir.expression.DBSPExpression;
import org.dbsp.sqlCompiler.ir.type.DBSPType;
import org.dbsp.sqlCompiler.ir.type.DBSPTypeIndexedZSet;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

/**
 * Apply a topK operation to each of the groups in an indexed collection.
 * This always sorts the elements of each group.
 * To sort the entire collection just group by ().
 */
public class DBSPIndexedTopKOperator extends DBSPUnaryOperator {
    /**
     * Limit K used by TopK.  Expected to be a constant.
     */
    public final DBSPExpression limit;
    /**
     * Optional closure which produces the output tuple.  The signature is
     * (i64, sorted_tuple) -> output_tuple.  i64 is the rank of the current row.
     * If this closure is missing it is assumed to produce just the sorted_tuple.
     */
    @Nullable
    public final DBSPClosureExpression outputProducer;

    static String operatorName(@Nullable DBSPClosureExpression outputProducer) {
        if (outputProducer == null)
            return "topk_custom_order";
        return "topk_rank_custom_order";
    }

    static DBSPType outputType(DBSPType sourceType, @Nullable DBSPClosureExpression outputProducer) {
        if (outputProducer == null)
            return sourceType;
        DBSPTypeIndexedZSet ix = sourceType.to(DBSPTypeIndexedZSet.class);
        return new DBSPTypeIndexedZSet(sourceType.getNode(), ix.keyType, outputProducer.getResultType(), ix.weightType);
    }

    /**
     * Create an IndexedTopK operator.  This operator is incremental only.
     * For a non-incremental version it should be sandwiched between a D-I.
     * @param node            CalciteObject which produced this operator.
     * @param function        A ComparatorExpression used to sort items in each group.
     * @param limit           Max number of records output in each group.
     * @param outputProducer  Optional function with signature (rank, tuple) which produces the output.
     * @param source          Input operator.
     */
    public DBSPIndexedTopKOperator(CalciteObject node, DBSPExpression function, DBSPExpression limit,
                                   @Nullable DBSPClosureExpression outputProducer, DBSPOperator source) {
        super(node, operatorName(outputProducer), function,
                outputType(source.outputType, outputProducer), source.isMultiset, source);
        this.limit = limit;
        this.outputProducer = outputProducer;
        if (!this.outputType.is(DBSPTypeIndexedZSet.class))
            throw new InternalCompilerError("Expected the input to be an IndexedZSet type", source.outputType);
        if (!function.is(DBSPComparatorExpression.class))
            throw new InternalCompilerError("Expected a comparator expression", function);
    }

    @Override
    public DBSPOperator withInputs(List<DBSPOperator> newInputs, boolean force) {
        if (force || this.inputsDiffer(newInputs))
            return new DBSPIndexedTopKOperator(this.getNode(), this.getFunction(),
                    this.limit, this.outputProducer, newInputs.get(0));
        return this;
    }

    @Override
    public DBSPOperator withFunction(@Nullable DBSPExpression expression, DBSPType outputType) {
        return new DBSPIndexedTopKOperator(this.getNode(), Objects.requireNonNull(expression), this.limit,
                this.outputProducer, this.input());
    }

    @Override
    public void accept(CircuitVisitor visitor) {
        if (visitor.preorder(this).stop()) return;
        visitor.postorder(this);
    }
}