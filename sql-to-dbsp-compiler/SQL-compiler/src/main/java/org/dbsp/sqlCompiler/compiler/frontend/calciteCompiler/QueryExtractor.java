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

 package org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.SqlNodeList;
import org.dbsp.sqlCompiler.compiler.frontend.statements.FrontEndStatement;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.stream.Collectors;

public class QueryExtractor {

    public QueryExtractor() {
    }

    private String buildSelectStatement(SqlNode sqlNode, String tableName, SqlCreateFunctionDeclaration decl) {
        StringBuilder builder = new StringBuilder();
        SqlSelect select = (SqlSelect) sqlNode;
    
        builder.append("SELECT ")
               .append(buildSelectList(select.getSelectList()))
               .append("\nFROM ")
               .append(buildFromClause(select.getFrom()))
               .append(buildWhereClause(select.getWhere(), decl.getParameters(), tableName))
               .append(buildGroupByClause(select.getGroup()))
               .append(buildHavingClause(select.getHaving(), decl.getParameters(), tableName));
    
        return builder.toString();
    }
    
    private String buildSelectList(List<SqlNode> selectList) {
        return selectList.stream()
                         .map(node -> node.toString().replace("`", ""))
                         .collect(Collectors.joining(", "));
    }
    
    private String buildFromClause(SqlNode fromNode) {
        if (fromNode instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) fromNode;
            return join.getLeft().toString().replace("`", "") + " " + join.getJoinType().name() + " JOIN " +
                   join.getRight().toString().replace("`", "") + " ON " + join.getCondition().toString().replace("`", "");
        } else {
            return fromNode.toString().replace("`", "");
        }
    }
    
    private String buildWhereClause(SqlNode whereNode, SqlNodeList parameters, String tableName) {
        if (whereNode == null || !(whereNode instanceof SqlBasicCall)) {
            return "";
        }
        String whereClause = "\nWHERE " + extractOperands(whereNode, parameters, tableName).replace("`", "");
        if (!parameters.isEmpty()) {
            whereClause = ", " + tableName + whereClause;
        }
        return whereClause;
    }
    
    private String buildGroupByClause(List<SqlNode> groupList) {
        if (groupList == null || groupList.isEmpty()) {
            return "";
        }
        return "\nGROUP BY " + groupList.stream()
                                         .map(SqlNode::toString)
                                         .collect(Collectors.joining(", "));
    }
    
    private String buildHavingClause(SqlNode havingNode, SqlNodeList parameters, String tableName) {
        if (havingNode == null || !(havingNode instanceof SqlBasicCall)) {
            return "";
        }
        return "\nHAVING " + extractOperands(havingNode, parameters, tableName).replace("`", "");
    }

    private String extractOperands(SqlNode node, SqlNodeList parameters, String tableName) {
        StringBuilder expression = new StringBuilder();

        if (node instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) node;
            SqlOperator operator = call.getOperator();
            List<SqlNode> operands = call.getOperandList();
            boolean isAggregateFunction = operator instanceof SqlFunction &&
                    (operator instanceof SqlUnresolvedFunction || operator instanceof SqlAggFunction);

            // Handle each operand recursively
            for (int i = 0; i < operands.size(); i++) {
                if (i > 0 && !isAggregateFunction) {
                    // Add operator between operands
                    expression.append(" ").append(operator).append(" ");
                }

                if (operands.get(i) instanceof SqlBasicCall) {
                    expression.append("(");
                }

                expression.append(extractOperands(operands.get(i), parameters, tableName));

                if (operands.get(i) instanceof SqlBasicCall) {
                    expression.append(")");
                }
            }

            if (isAggregateFunction) {
                expression.insert(0, operator + "(").append(")");
            }
        } else {
            // Handle leaf nodes (non-operator nodes)
            boolean appended = false;
            for (SqlNode column : parameters) {
                String columnName = column.toString().split(" ")[0].replace("`", "");
                if (columnName.toLowerCase().equals(node.toString().toLowerCase())) {
                    expression.append(tableName).append(".").append(node.toString());
                    appended = true;
                    break;
                }
            }
            if (!appended) {
                expression.append(node.toString());
            }
        }

        return expression.toString();
    }

    // Recursive method to process SQL nodes
    private String processSqlNode(SqlNode sqlNode, String tableName, SqlCreateFunctionDeclaration decl) {
        StringBuilder result = new StringBuilder();

        if (sqlNode instanceof SqlSelect) {
            result.append(buildSelectStatement(sqlNode, tableName, decl));
        } else if (sqlNode instanceof SqlBasicCall) {
            processSqlBasicCall((SqlBasicCall) sqlNode, result, tableName, decl);
        } else if (sqlNode instanceof SqlOrderBy) {
            processSqlOrderBy((SqlOrderBy) sqlNode, result, tableName, decl);
        }

        return result.toString();
    }

    private void processSqlBasicCall(SqlBasicCall basicCall, StringBuilder result, String tableName, SqlCreateFunctionDeclaration decl) {
        SqlOperator operator = basicCall.getOperator();
        if (operator instanceof SqlSetOperator) {
            SqlSetOperator setOperator = (SqlSetOperator) operator;

            result.append(processSqlNode(basicCall.getOperandList().get(0), tableName, decl));
            result.append("\n").append(setOperator.toString()).append("\n");
            result.append(processSqlNode(basicCall.getOperandList().get(1), tableName, decl));
        }
    }

    private void processSqlOrderBy(SqlOrderBy orderBy, StringBuilder result, String tableName, SqlCreateFunctionDeclaration decl) {
        SqlNode query = orderBy.query;
        if (query instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) query;
            if (basicCall.getOperator() instanceof SqlSetOperator) {
                processSqlBasicCall(basicCall, result, tableName, decl);
            }
        } else {
            result.append(buildSelectStatement(query, tableName, decl));
        }
        appendOrderByClause(orderBy, result);
    }

    private void appendOrderByClause(SqlOrderBy orderBy, StringBuilder result) {
        SqlNodeList orderList = orderBy.orderList;
        result.append("\nORDER BY ");
        String orderByString = orderList.stream()
                                        .map(node -> node.toString().replace("`", ""))
                                        .collect(Collectors.joining(", "));
        result.append(orderByString);
    }

    private boolean isAggregate(SqlCreateFunctionDeclaration decl) {
        // Determines if the function declaration is an aggregate function based on the return type
        return decl.getReturnType() != null;
    }

    private String createInlineQueryFunction(SqlCreateFunctionDeclaration decl, SqlIdentifier name) {
        SqlNode body = decl.getBody();
        if (body == null) {
            return null;
        }
    
        StringBuilder builder = new StringBuilder();
        String tableName = name.toString() + "_" + decl.getName().toString() + "_INPUT";
        String viewName = name.toString() + "_" + decl.getName().toString();
        String sanitizedBody = body.toString().replace("`", "");
        
        SqlParser parser = SqlParser.create(sanitizedBody);
    
        try {
            // Parse the SQL query
            SqlNode sqlNode = parser.parseQuery();
            builder.append("CREATE VIEW ").append(viewName).append(" AS\n");
            builder.append(processSqlNode(sqlNode, tableName, decl));
    
            if (isAggregate(decl)) {
                appendGroupByClause(builder, decl);
            }
    
            builder.append(";\n\n");
        } catch (SqlParseException e) {
            e.printStackTrace();
            return null;
        }
        return builder.toString();
    }
    
    private void appendGroupByClause(StringBuilder builder, SqlCreateFunctionDeclaration decl) {
        builder.append("\nGROUP BY ");
        String groupByClause = decl.getParameters().stream()
                                   .map(parameter -> parameter.toString().split(" ")[0].replace("`", ""))
                                   .collect(Collectors.joining(", "));
        builder.append(groupByClause);
    }

    public String generateStatements(SqlIdentifier name, SqlNode statement, List<SqlNode> inlineQueryNodes) {
        StringBuilder builder = new StringBuilder();
        SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config(), builder);
    
        List<String> viewNames = new ArrayList<>();
        List<String> tableNames = new ArrayList<>();
        List<SqlCreateFunctionDeclaration> declarations = findFunctionDeclarations(statement, inlineQueryNodes);

        for (SqlCreateFunctionDeclaration decl : declarations) {
            
            if (decl == null) {
                throw new RuntimeException("Function declaration not found.");
            }

            String tableName = name.toString() + "_" + decl.getName().toString() + "_INPUT";
            String viewName = name.toString() + "_" + decl.getName().toString() + "_OUTPUT";

            viewNames.add(viewName);
            tableNames.add(tableName);

            SqlParser parser = SqlParser.create(statement.toString().replace("`", ""));
            try {
                SqlNode sqlNode = parser.parseQuery();

                if (sqlNode instanceof SqlSelect) {
                    SqlSelect select = (SqlSelect) sqlNode;

                    // Create proxy table as the function input
                    appendInputTableStatement(builder, tableName, select, decl, writer);
                    // Insert data into proxy table
                    builder.append(createInlineQueryFunction(decl, name));
                }
            } catch (SqlParseException e) {
                e.printStackTrace();
            }
        }

        // Create view as the function output
        appendCreateViewStatement(builder, name, tableNames, viewNames, statement, declarations);
        return builder.toString();
    }
    
    private List<SqlCreateFunctionDeclaration> findFunctionDeclarations(SqlNode statement, List<SqlNode> inlineQueryNodes) {
        List<SqlCreateFunctionDeclaration> functionDeclarations = new ArrayList<>();
    
        if (!(statement instanceof SqlSelect)) {
            throw new IllegalArgumentException("Statement must be an instance of SqlSelect");
        }
    
        SqlSelect selectStatement = (SqlSelect) statement;
    
        for (SqlNode inlineNode : inlineQueryNodes) {
            if (!(inlineNode instanceof SqlCreateFunctionDeclaration)) {
                continue;
            }
    
            SqlCreateFunctionDeclaration functionDeclaration = (SqlCreateFunctionDeclaration) inlineNode;
    
            for (SqlNode selectNode : selectStatement.getSelectList()) {
                if (selectNode instanceof SqlCall) {
                    SqlCall call = (SqlCall) selectNode;
                    SqlOperator operator = getOperatorFromCall(call);
    
                    if (operator != null && operator.toString().toLowerCase().equals(functionDeclaration.getName().toString().toLowerCase())) {
                        functionDeclarations.add(functionDeclaration);
                    }
                }
            }
        }
    
        return functionDeclarations;
    }
    
    private SqlOperator getOperatorFromCall(SqlCall call) {
        SqlOperator operator = call.getOperator();
    
        if (operator != null && "AS".equals(operator.toString())) {
            List<SqlNode> operands = call.getOperandList();
            if (!operands.isEmpty() && operands.get(0) instanceof SqlCall) {
                return ((SqlCall) operands.get(0)).getOperator();
            }
        }
    
        return operator;
    }
    
    private void appendWhereClause(StringBuilder builder, SqlNode whereNode, SqlCreateFunctionDeclaration decl, String tableName) {
        if (whereNode == null || !(whereNode instanceof SqlBasicCall)) {
            return;
        }
        String whereClause = " WHERE " + extractOperands(whereNode, decl.getParameters(), tableName).replace("`", "");
        builder.append(whereClause);
    }

    private void appendInputTableStatement(StringBuilder builder, String tableName, SqlSelect select, SqlCreateFunctionDeclaration decl, SqlWriter writer) {
        builder.append("CREATE VIEW ").append(tableName).append("(");
    
        for (int i = 0; i < decl.getParameters().size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            String parameter = decl.getParameters().get(i).toString().split(" ")[0].replace("`", "");
            builder.append(parameter);
        }
    
        List<String> parameterList = extractParameters(select, decl);
        //TODO: what do I do when the parameter list is empty?
     
        builder.append(") AS\nSELECT DISTINCT ");
        appendParameterList(builder, parameterList);
    
        builder.append(" FROM ").append(select.getFrom().toString());
        appendWhereClause(builder, select.getWhere(), decl, tableName);
        builder.append(";\n\n");
    }
    
    private void appendParameterList(StringBuilder builder, List<String> parameterList) {
        for (int i = 0; i < parameterList.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(parameterList.get(i));
        }
    }
    
    private List<String> extractParameters(SqlSelect select, SqlCreateFunctionDeclaration decl) {
        List<String> parameterList = new ArrayList<>();
    
        for (SqlNode selectNode : select.getSelectList()) {

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
    
        return parameterList;
    }
    
    private void addOperandsToList(List<String> parameterList, List<SqlNode> operands) {
        for (SqlNode operand : operands) {
            parameterList.add(operand.toString());
        }
    }
    
    private void appendCreateViewStatement(StringBuilder builder, SqlIdentifier name, List<String> tableNames, List<String> viewNames, SqlNode statement, List<SqlCreateFunctionDeclaration> declarations) {
        builder.append("CREATE VIEW ").append(name).append(" AS\n");
    
        for (int i = 0; i < declarations.size(); i++) {
            SqlCreateFunctionDeclaration decl = declarations.get(i);
            String tableName = tableNames.get(i);
            String viewName = viewNames.get(i);
    
            if (!isAggregate(decl)) {
                if (i > 0) {
                    builder.append("UNION ALL\n");
                }
                builder.append("SELECT * FROM ").append(viewName).append("\n");
            } else {
                appendAggregateFunction(builder, statement, decl, tableName, i, name);
            }
        }
        
        appendFromClause(builder, tableNames);
    }
    
    private void appendAggregateFunction(StringBuilder builder, SqlNode statement, SqlCreateFunctionDeclaration decl, String tableName, int index, SqlIdentifier viewName) {
        if (index == 0) {
            builder.append("SELECT ");
        } else {
            builder.append(", ");
        }
    
        SqlParser parser = SqlParser.create(statement.toString().replace("`", ""));
        try {
            SqlNode sqlNode = parser.parseQuery();
            if (sqlNode instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) sqlNode;
                List<String> parameterList = extractParameters(select, decl);
    
                for (int k = 0; k < decl.getParameters().size(); k++) {
                    if (k > 0) {
                        builder.append(", ");
                    }
                    String functionParameter = tableName + "." + decl.getParameters().get(k).toString().split(" ")[0].replace("`", "");
                    builder.append(functionParameter).append(" AS ").append(parameterList.get(k).replace("'", "")).append("_").append(index + 1);
                }
    
                builder.append(", (SELECT * FROM ").append(viewName.toString()).append("_").append(decl.getName().toString()).append(")");;
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }
    
    private void appendFromClause(StringBuilder builder, List<String> tableNames) {
        builder.append("\nFROM ");
        for (int i = 0; i < tableNames.size(); i++) {
            if (i > 0) {
                builder.append(", ");
            }
            builder.append(tableNames.get(i));
        }
        builder.append(";\n");
    }
}