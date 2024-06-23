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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.SqlNodeList;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlCreateFunctionDeclaration;

import java.util.stream.Collectors;
import java.util.List;

public class FunctionBodyParser extends BaseQueryExtractor {

    public FunctionBodyParser(SqlIdentifier name, SqlCreateFunctionDeclaration declaration) {
        super(name, declaration);
    }

    public String createInlineQueryFunction() {
        try {
            SqlParser parser = SqlParser.create(decl.getBody().toString().replace("`", ""));
            SqlNode sqlNode = parser.parseQuery();

            if (returnsSingleValue()) {
                builder.append("CREATE VIEW ").append(intermediateView).append(" AS\n");
            } else {
                builder.append("CREATE VIEW ").append(finalView).append(" AS\n");
            }

            builder.append(processSqlNode(sqlNode));

            if (isAggregate()) {
                appendGroupByClause();
            }

            builder.append(";\n\n");
            return builder.toString();
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        return "";
    }

    private String buildSelectStatement(SqlNode sqlNode) {
        StringBuilder builderAlt = new StringBuilder("SELECT ");
        SqlSelect select = (SqlSelect) sqlNode;

        if (returnsSingleValue()) {
            String groupByClause = decl.getParameters().stream()
                                   .map(parameter -> parameter.toString().split(" ")[0].replace("`", ""))
                                   .collect(Collectors.joining(", "));
            builderAlt.append(groupByClause).append(", ");
        }

        builderAlt.append(buildSelectList(select.getSelectList()))
               .append("\nFROM ")
               .append(buildFromClause(select.getFrom()))
               .append(buildWhereClause(select.getWhere(),null))
               .append(buildGroupByClause(select.getGroup()))
               .append(buildHavingClause(select.getHaving(), null));

        return builderAlt.toString();
    }

    private String buildSelectList(List<SqlNode> selectList) {
        return selectList.stream()
                .map(node -> node.toString().replace("`", "") +
                    (selectList.size() == 1 && !node.toString().equals("*") ? " AS " + alias : ""))
                .collect(Collectors.joining(", "));
    }

    protected String buildFromClause(SqlNode fromNode) {
        if (fromNode instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) fromNode;
            return join.getLeft().toString().replace("`", "") + " " + join.getJoinType().name() + " JOIN " +
                    join.getRight().toString().replace("`", "") + " ON " + join.getCondition().toString().replace("`", "");
        } else {
            return fromNode.toString().replace("`", "");
            // return tempTable;
        }
    }

    protected String buildWhereClause(SqlNode whereNode, SqlNode fromNode) {
        if (whereNode == null || !(whereNode instanceof SqlBasicCall)) {
            return "";
        }
        String whereClause = "\nWHERE " + extractOperands(whereNode, fromNode).replace("`", "");
        if (!functionArguments.isEmpty()) {
            whereClause = ", " + tableWithFunctionArguments + whereClause;
        }
        return whereClause;
    }

    protected String buildGroupByClause(List<SqlNode> groupList) {
        if (groupList == null || groupList.isEmpty()) {
            return "";
        }
        return "\nGROUP BY " + groupList.stream()
                .map(SqlNode::toString)
                .collect(Collectors.joining(", "));
    }

    protected String buildHavingClause(SqlNode havingNode, SqlNode fromNode) {
        if (havingNode == null || !(havingNode instanceof SqlBasicCall)) {
            return "";
        }
        return "\nHAVING " + extractOperands(havingNode, fromNode).replace("`", "");
    }

    // Method that recursively processes the Function body
    private String processSqlNode(SqlNode sqlNode) {
        StringBuilder result = new StringBuilder();

        if (sqlNode instanceof SqlSelect) {
            result.append(buildSelectStatement(sqlNode));
        } else if (sqlNode instanceof SqlBasicCall) {
            processSqlBasicCall((SqlBasicCall) sqlNode, result);
        } else if (sqlNode instanceof SqlOrderBy) {
            processSqlOrderBy((SqlOrderBy) sqlNode, result);
        }

        return result.toString();
    }

    //Handle functions that use set operators, such as UNION, INTERSECT, EXCEPT
    private void processSqlBasicCall(SqlBasicCall basicCall, StringBuilder result) {
        SqlOperator operator = basicCall.getOperator();
        if (operator instanceof SqlSetOperator) {
            SqlSetOperator setOperator = (SqlSetOperator) operator;

            result.append(processSqlNode(basicCall.getOperandList().get(0)));
            result.append("\n").append(setOperator.toString()).append("\n");
            result.append(processSqlNode(basicCall.getOperandList().get(1)));
        }
    }

    //Handle functions that use the ORDER BY clause
    private void processSqlOrderBy(SqlOrderBy orderBy, StringBuilder result) {
        SqlNode query = orderBy.query;
        if (query instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) query;
            if (basicCall.getOperator() instanceof SqlSetOperator) {
                processSqlBasicCall(basicCall, result);
            }
        } else {
            result.append(buildSelectStatement(query));
        }
        appendOrderByClause(orderBy, result);
    }

    private void appendOrderByClause(SqlOrderBy orderBy, StringBuilder result) {
        SqlNodeList orderList = orderBy.orderList;
        result.append("\nORDER BY ").append(orderList.stream()
                .map(node -> node.toString().replace("`", ""))
                .collect(Collectors.joining(", ")));
    }

    private void appendGroupByClause() {
        builder.append("\nGROUP BY ").append(decl.getParameters().stream()
                .map(parameter -> parameter.toString().split(" ")[0].replace("`", ""))
                .collect(Collectors.joining(", ")));
    }

    public void setFunctionDeclaration(SqlCreateFunctionDeclaration decl) {
        this.decl = decl;
        this.functionArguments = decl.getParameters();
    }

    public void setAlias(String alias) {
        this.alias = alias;
    }
}