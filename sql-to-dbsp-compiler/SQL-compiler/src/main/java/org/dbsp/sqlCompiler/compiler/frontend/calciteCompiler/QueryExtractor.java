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
    private StringBuilder builder;
    private SqlCreateFunctionDeclaration decl;
    private String tempTable;
    private String tableWithFunctionArguments;
    private String intermediateView;
    private String finalView;
    private SqlNodeList functionArguments;

    public QueryExtractor() {
    }

    private String buildSelectStatement(SqlNode sqlNode) {
        StringBuilder builderAlt = new StringBuilder("SELECT ");
        SqlSelect select = (SqlSelect) sqlNode;

        if (isAggregate()) {
            String groupByClause = decl.getParameters().stream() 
                                   .map(parameter -> parameter.toString().split(" ")[0].replace("`", ""))
                                   .collect(Collectors.joining(", "));
            builderAlt.append(groupByClause).append(", ");
        }

        builderAlt.append(buildSelectList(select.getSelectList()))
               .append("\nFROM ")
               .append(buildFromClause(select.getFrom()))
               .append(buildWhereClause(select.getWhere()))
               .append(buildGroupByClause(select.getGroup()))
               .append(buildHavingClause(select.getHaving()));
    
        return builderAlt.toString();
    }
    
    private String buildSelectList(List<SqlNode> selectList) {
        return selectList.stream()
                .map(node -> node.toString().replace("`", "") + 
                    (selectList.size() == 1 && !node.toString().equals("*") ? " AS FUNCTION_OUTPUT" : ""))
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
    
    private String buildWhereClause(SqlNode whereNode) {
        if (whereNode == null || !(whereNode instanceof SqlBasicCall)) {
            return "";
        }
        String whereClause = "\nWHERE " + extractOperands(whereNode).replace("`", "");
        if (!functionArguments.isEmpty()) {
            whereClause = ", " + tableWithFunctionArguments + whereClause;
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

    private String buildHavingClause(SqlNode havingNode) {
        if (havingNode == null || !(havingNode instanceof SqlBasicCall)) {
            return "";
        }
        return "\nHAVING " + extractOperands(havingNode).replace("`", "");
    }

    private String extractOperands(SqlNode node) {
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

                expression.append(extractOperands(operands.get(i)));

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
            for (SqlNode column : functionArguments) {
                String columnName = column.toString().split(" ")[0].replace("`", "");
                if (columnName.toLowerCase().equals(node.toString().toLowerCase())) {
                    expression.append(tableWithFunctionArguments).append(".").append(node.toString());
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

    private void processSqlBasicCall(SqlBasicCall basicCall, StringBuilder result) {
        SqlOperator operator = basicCall.getOperator();
        if (operator instanceof SqlSetOperator) {
            SqlSetOperator setOperator = (SqlSetOperator) operator;

            result.append(processSqlNode(basicCall.getOperandList().get(0)));
            result.append("\n").append(setOperator.toString()).append("\n");
            result.append(processSqlNode(basicCall.getOperandList().get(1)));
        }
    }

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

    private boolean isAggregate() {
        // Determines if the function declaration is an aggregate function based on the return type and the function body
        return decl.getReturnType() != null;
    }

    private void createInlineQueryFunction() {
        try {
            SqlParser parser = SqlParser.create(decl.getBody().toString().replace("`", ""));
            SqlNode sqlNode = parser.parseQuery();

            if (isAggregate()) {
                builder.append("CREATE VIEW ").append(intermediateView).append(" AS\n");
            } else {
                builder.append("CREATE VIEW ").append(finalView).append(" AS\n");
            }

            builder.append(processSqlNode(sqlNode));

            if (isAggregate()) {
                appendGroupByClause();
            }

            builder.append(";\n\n");
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }

    private void appendGroupByClause() {
        builder.append("\nGROUP BY ").append(decl.getParameters().stream()
                .map(parameter -> parameter.toString().split(" ")[0].replace("`", ""))
                .collect(Collectors.joining(", ")));
    }

    public String generateStatements(SqlIdentifier name, SqlNode statement, List<SqlNode> inlineQueryNodes) {
        builder = new StringBuilder();
        SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config(), builder);
        System.out.println(statement.toString());
        List<SqlCreateFunctionDeclaration> declarations = findFunctionDeclarations(statement, inlineQueryNodes);
        if (declarations.isEmpty()) {
            throw new RuntimeException("Function declaration not found.");
        }
        //TODO: Parse multiple function declarations
        decl = declarations.get(0);
        functionArguments = decl.getParameters();
        finalView = name.toString();

        tableWithFunctionArguments = finalView + "_" + decl.getName().toString() + "_INPUT";
        intermediateView = finalView + "_" + decl.getName().toString();
        

        SqlParser parser = SqlParser.create(statement.toString().replace("`", ""));
        try {
            SqlNode sqlNode = parser.parseQuery();

            if (sqlNode instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) sqlNode;
                //Create proxy table that copies the source table
                appendProxyTableStatement(select);
                //Create proxy input table as the function input
                appendInputTableStatement(select, writer);
                //Insert data into proxy table
                createInlineQueryFunction();
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }

        // Create view as the function output
        if (isAggregate()) appendCreateViewStatement(statement);
        System.out.println(builder.toString()); 
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
    
    private void appendWhereClause(SqlNode whereNode) {
        if (whereNode == null || !(whereNode instanceof SqlBasicCall)) {
            return;
        }
        String whereClause = " WHERE " + extractOperands(whereNode).replace("`", "");
        builder.append(whereClause);
    }
    
    private void appendProxyTableStatement(SqlSelect select) {
        tempTable = finalView + "_" + select.getFrom().toString().replace("`", "") + "_PROXY";
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

    private void appendAggregateFunction(SqlNode statement, int index) {
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
                List<String> parameterList = extractParameters(select);
    
                for (int k = 0; k < functionArguments.size(); k++) {
                    if (k > 0) {
                        builder.append(", ");
                    }
                    String functionParameter = tableWithFunctionArguments + "." + functionArguments.get(k).toString().split(" ")[0].replace("`", "");
                    builder.append(functionParameter).append(" AS ").append(parameterList.get(k).replace("'", "")).append("_").append(index + 1);
                }
                

                builder.append(", (SELECT * FROM ").append(intermediateView).append("_").append(decl.getName().toString()).append(")");;
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
    }
}