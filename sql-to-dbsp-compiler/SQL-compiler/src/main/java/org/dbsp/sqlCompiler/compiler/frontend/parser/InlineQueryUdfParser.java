/*
 * Copyright 2022 VMware, Inc.
 * SPDX-License-Identifier: MIT
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package org.dbsp.sqlCompiler.compiler.frontend.parser;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlCreateFunctionDeclaration;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class InlineQueryUdfParser extends BaseQueryExtractor {

    private final FunctionBodyParser functionBodyParser;

    public InlineQueryUdfParser(SqlIdentifier name, SqlNode statement, List<SqlNode> inlineQueryNodes) {
        super(name, statement, inlineQueryNodes);
        this.functionBodyParser = new FunctionBodyParser(name, statement, inlineQueryNodes);
    }

    public String generateStatements(SqlNode statement) {
        SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config(), builder);

        try {
            SqlParser parser = SqlParser.create(statement.toString().replace("`", ""));
            SqlNode sqlNode = parser.parseQuery();

            if (sqlNode instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) sqlNode;
                appendProxyTableStatement(select);
                appendInputTableStatement(select, writer);
                builder.append(functionBodyParser.createInlineQueryFunction());
                if (isAggregate()) appendCreateViewStatement(statement);
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        System.out.println(builder.toString());
        return builder.toString();
    }

    private void appendProxyTableStatement(SqlSelect select) {
        builder.append("CREATE VIEW ").append(tempTable).append(" AS SELECT * FROM ").append(select.getFrom().toString());
        appendWhereClause(select.getWhere());
        builder.append(";\n\n");
    }

    private void appendInputTableStatement(SqlSelect select, SqlWriter writer) {
        builder.append("CREATE VIEW ").append(tableWithFunctionArguments).append("(");
    
        List<String> viewParam = new ArrayList<String>();
        for (int i = 0; i < functionArguments.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            String parameter = functionArguments.get(i).toString().split(" ")[0].replace("`", "");
            viewParam.add(parameter);
            builder.append(parameter);
        }
    
        List<String> parameterList = extractParameters(select);
        //TODO: what do I do when the parameter list is empty?
     
        builder.append(") AS\nSELECT DISTINCT ");
        appendParameterList(parameterList, viewParam);
    
        builder.append(" FROM ").append(select.getFrom().toString());
        builder.append(";\n\n");
    }

    private void appendParameterList(List<String> parameterList, List<String> viewParam) {
        for (int i = 0; i < parameterList.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(parameterList.get(i) + " AS " + viewParam.get(i));
        }
    }

    private void appendCreateViewStatement(SqlNode statement) {
        builder.append("CREATE VIEW ").append(finalView).append(" AS\n");
        builder.append("SELECT ").append(extractParameters((SqlSelect) statement, true).stream().collect(Collectors.joining(", ")).replace("`", "")).append(", FUNCTION_OUTPUT\n");
        builder.append("FROM ").append(tempTable).append("\n");
        builder.append("JOIN ").append(intermediateView).append(" ON ");
        builder.append(appendOnClause(statement));
    }
    
    private String appendOnClause(SqlNode whereNode) {
        return tempTable + ".AGE = " + intermediateView + ".USERAGE;";
    }

    private List<String> extractParameters(SqlSelect select) {
        return extractParameters(select, false);
    }

    private List<String> extractParameters(SqlSelect select, boolean fetchAll) {
        List<String> parameterList = new ArrayList<>();

        for (SqlNode selectNode : select.getSelectList()) {
            if (fetchAll && selectNode instanceof SqlIdentifier) {
                parameterList.add(selectNode.toString());
                continue;
            }
            if (selectNode instanceof SqlCall) {
                SqlCall call = (SqlCall) selectNode;
                List<SqlNode> operands = call.getOperandList();
                SqlOperator function = call.getOperator();
                if (function == null) continue;

                if ("AS".equals(function.toString())) {
                    SqlCall functionCall = (SqlCall) operands.get(0);
                    SqlOperator functionOperator = functionCall.getOperator();
                    if (functionOperator != null && functionOperator.toString().toLowerCase().equals(decl.getName().toString().toLowerCase())) {
                        addOperandsToList(parameterList, functionCall.getOperandList());
                    }
                } else if (function.toString().toLowerCase().equals(decl.getName().toString().toLowerCase())) {
                    addOperandsToList(parameterList, operands);
                }
            }

        }
        return parameterList.stream().distinct().collect(Collectors.toList());
    }

    private void addOperandsToList(List<String> parameterList, List<SqlNode> operands) {
        for (SqlNode operand : operands) {
            parameterList.add(operand.toString());
        }
    }
    
}