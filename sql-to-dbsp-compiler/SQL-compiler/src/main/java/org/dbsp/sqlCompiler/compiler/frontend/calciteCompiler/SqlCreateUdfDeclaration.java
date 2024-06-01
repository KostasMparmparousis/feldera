package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Our own version of CREATE FUNCTION, different from Calcite. */
public class SqlCreateUdfDeclaration extends SqlCreate {
    private final SqlIdentifier name;
    private final SqlNodeList parameters;
    private final SqlDataTypeSpec returnType;
    private final SqlIdentifier res;
    private final SqlIdentifier source;
    private final @Nullable SqlNode expression;

    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE FUNCTION",
            SqlKind.CREATE_FUNCTION);

    public SqlCreateUdfDeclaration(SqlParserPos pos, boolean replace,
            boolean ifNotExists, SqlIdentifier name,
            SqlNodeList parameters, SqlDataTypeSpec returnType,
            SqlIdentifier res, SqlIdentifier source, @Nullable SqlNode expression) {
        super(OPERATOR, pos, replace, ifNotExists);
        this.name = Objects.requireNonNull(name, "name");
        this.parameters = Objects.requireNonNull(parameters, "parameters");
        this.returnType = returnType;
        this.res = res;
        this.source = source;
        this.expression = expression;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec,
            int rightPrec) {
        writer.keyword(getReplace() ? "CREATE OR REPLACE" : "CREATE");
        writer.keyword("FUNCTION");
        if (this.ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        this.name.unparse(writer, 0, 0);
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
        for (SqlNode parameter : this.parameters) {
            writer.sep(",");
            parameter.unparse(writer, 0, 0);
        }
        writer.endList(frame);
        writer.keyword("RETURNS");
        this.returnType.unparse(writer, 0, 0);
        writer.keyword("AS");
        final SqlWriter.Frame fr = writer.startList(SqlWriter.FrameTypeEnum.SIMPLE);
        writer.keyword("SELECT");
        this.res.unparse(writer, 0, 0);
        writer.keyword("FROM");
        this.source.unparse(writer, 0, 0);
        if (this.expression != null) {
            writer.keyword("WHERE");
            this.expression.unparse(writer, 0, 0);
        }
        writer.endList(fr);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public SqlNodeList getParameters() {
        return this.parameters;
    }

    public SqlDataTypeSpec getReturnType() {
        return this.returnType;
    }

    public SqlIdentifier getName() {
        return this.name;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(this.name, this.parameters);
    }

    public SqlIdentifier getRes() {
        return this.res;
    }

    public SqlIdentifier getSource() {
        return this.source;
    }

    public SqlNode getExpression() {
        return this.expression;
    }

}
