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
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.CalciteCompiler;


import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;

public class QueryExtractor {
    private final CalciteCompiler frontend;
    public QueryExtractor(CalciteCompiler frontend) {
        this.frontend = frontend;
    }

    private String extractOperands(SqlNode node, SqlNodeList parameters, String tableName) {
        StringBuilder expression = new StringBuilder();

        if (node instanceof SqlBasicCall) {
            SqlBasicCall call = (SqlBasicCall) node;
            SqlOperator operator = call.getOperator();
            List<SqlNode> operands = call.getOperandList();
            boolean isAggregateFunction = false;
            if (operator instanceof SqlFunction) {
                isAggregateFunction = operator instanceof SqlUnresolvedFunction || operator instanceof SqlAggFunction;
            }

            // Handle each operand recursively
            for (int i = 0; i < operands.size(); i++) {
                if (i > 0 && !isAggregateFunction) {
                    // Add operator between operands
                    expression.append(" ").append(operator).append(" ");
                }

                boolean isNested = operands.get(i) instanceof SqlBasicCall;
                if (isNested) {
                    expression.append("(");
                }

                expression.append(extractOperands(operands.get(i), parameters, tableName));
                if (isNested) {
                    expression.append(")");
                }
            }

            if (isAggregateFunction) {
                expression.insert(0, operator + "(");
                expression.append(")");
            }
        } else {
            // Handle leaf nodes (non-operator nodes)
            boolean appended = false;
            Iterator<SqlNode> paramIterator = parameters.iterator();
            while (paramIterator.hasNext()) {
                SqlNode column = paramIterator.next();
                String columnString = column.toString();
                String columnName = columnString.split(" ")[0].replace("`", "");
                if (columnName.equals(node.toString())) {
                    expression.append(tableName + "." + node.toString());
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

    private String buildSelectStatement(SqlNode sqlNode, String tableName, SqlCreateFunctionDeclaration decl) {
        StringBuilder builder = new StringBuilder();
        SqlSelect select = (SqlSelect) sqlNode;

        String selectString = "SELECT ";
        int count = 0;
        // Get the SELECT identifier
        for (SqlNode selectNode : select.getSelectList()) {
            if (count++ > 0) {
                selectString += ", ";
            }
            selectString += selectNode.toString().replace("`", "");
        }

        builder.append(selectString);

        // Get the FROM identifier
        SqlNode fromNode = select.getFrom();
        if (fromNode instanceof SqlJoin) {
            SqlJoin join = (SqlJoin) fromNode;
            SqlNode left = join.getLeft();
            SqlNode right = join.getRight();
            SqlNode condition = join.getCondition();
            JoinType joinType = join.getJoinType();

            String joinString = "\nFROM " + left.toString() + " " + joinType.name() + " JOIN "
                    + right.toString()
                    + " ON " + condition.toString();
            joinString = joinString.replace("`", "");
            builder.append(joinString);
        } else
            builder.append("\nFROM " + fromNode.toString());

        // Get the WHERE condition
        SqlNode whereNode = select.getWhere();

        if (whereNode != null && whereNode instanceof SqlBasicCall) {
            if (decl.getParameters().size() > 0)
                builder.append(", " + tableName);
            builder.append("\nWHERE ");

            String whereClause = extractOperands(whereNode, decl.getParameters(), tableName);
            builder.append(whereClause.replace("`", ""));
        }

        if (select.getGroup() != null && select.getGroup().size() > 0) {
            String groupByString = "GROUP BY ";
            count = 0;
            // Get the GROUP BY identifier
            for (SqlNode groupByNode : select.getGroup()) {
                if (count++ > 0) {
                    groupByString += ", ";
                }
                groupByString += groupByNode.toString();
            }

            if (count > 0) {
                builder.append("\n" + groupByString);
            }

            // Get the HAVING condition
            SqlNode havingNode = select.getHaving();

            if (havingNode != null && havingNode instanceof SqlBasicCall) {
                builder.append("\nHAVING ");

                String havingClause = extractOperands(havingNode, decl.getParameters(), tableName);
                // modify extractOperands to include aggregate functions
                builder.append(havingClause.replace("`", ""));
            }
        }

        return builder.toString();
    }

    // Recursive method to process SQL nodes
    private String processSqlNode(SqlNode sqlNode, String tableName, SqlCreateFunctionDeclaration decl) {
        StringBuilder result = new StringBuilder();

        if (sqlNode instanceof SqlSelect) {
            result.append(buildSelectStatement(sqlNode, tableName, decl));
        } else if (sqlNode instanceof SqlBasicCall) {
            SqlBasicCall basicCall = (SqlBasicCall) sqlNode;
            if (basicCall.getOperator() instanceof SqlSetOperator) {
                SqlSetOperator setOperator = (SqlSetOperator) basicCall.getOperator();

                result.append(processSqlNode(basicCall.getOperandList().get(0), tableName, decl));

                String operator = setOperator.toString();
                result.append("\n").append(operator).append("\n");

                result.append(processSqlNode(basicCall.getOperandList().get(1), tableName, decl));
            }
        } else if (sqlNode instanceof SqlOrderBy) {
            SqlOrderBy orderBy = (SqlOrderBy) sqlNode;
            if (orderBy.query instanceof SqlBasicCall) {
                SqlBasicCall basicCall = (SqlBasicCall) orderBy.query;
                if (basicCall.getOperator() instanceof SqlSetOperator) {
                    SqlSetOperator setOperator = (SqlSetOperator) basicCall.getOperator();

                    result.append(processSqlNode(basicCall.getOperandList().get(0), tableName, decl));

                    String operator = setOperator.toString();
                    result.append("\n").append(operator).append("\n");

                    result.append(processSqlNode(basicCall.getOperandList().get(1), tableName, decl));
                    SqlNodeList orderList = orderBy.orderList;
                    result.append("\nORDER BY ");
                    int count = 0;
                    for (SqlNode orderItem : orderList) {
                        if (count++ > 0) {
                            result.append(", ");
                        }
                        result.append(orderItem.toString().replace("`", ""));
                    }
                }
            } else {
                // Handle regular ORDER BY
                result.append(buildSelectStatement(orderBy.query, tableName, decl));
                SqlNodeList orderList = orderBy.orderList;
                result.append("\nORDER BY ");
                int count = 0;
                for (SqlNode orderItem : orderList) {
                    if (count++ > 0) {
                        result.append(", ");
                    }
                    result.append(orderItem.toString().replace("`", ""));
                }
            }
        }

        return result.toString();
    }

    private boolean isAggregate(SqlCreateFunctionDeclaration decl) {
        // TODO: Function body
        if (decl.getReturnType() == null)
            return false;
        return true;
    }

    private String createInlineQueryFunction(SqlCreateFunctionDeclaration decl, SqlIdentifier name) {
        SqlNode body = decl.getBody();
        if (body == null)
            return null;

        StringBuilder builder = new StringBuilder();
        SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config(), builder);
        String tableName = name.toString() + "_" + decl.getName().toString() + "_INPUT";
        String viewName = name.toString() + "_" + decl.getName().toString();
        SqlParser parser = SqlParser.create(body.toString().replace("`", ""));
        try {
            // Parse the SQL query
            SqlNode sqlNode = parser.parseQuery();
            builder.append("CREATE VIEW " + viewName + " AS\n");
            builder.append(processSqlNode(sqlNode, tableName, decl));
            if (isAggregate(decl)) {
                builder.append("\nGROUP BY ");
                for (int i = 0; i < decl.getParameters().size(); i++) {
                    if (i > 0) {
                        builder.append(", ");
                    }
                    String functionParameter = decl.getParameters().get(i).toString().split(" ")[0].replace("`",
                            "");
                    builder.append(functionParameter);
                }
            }
            // later
            builder.append(";\n\n");

        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        String sql = builder.toString();
        // System.out.println(sql);
        return sql;
    }

    public List<FrontEndStatement> generateFrontEndStatements(SqlIdentifier name, SqlNode statement,
            List<SqlNode> inlineQueryNodes) {
        List<FrontEndStatement> result = new ArrayList<>();
        StringBuilder builder = new StringBuilder();
        SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config(), builder);       
        try {
            List<String> viewNames = new ArrayList<String>();
            List<String> tableNames = new ArrayList<String>();
            List<SqlCreateFunctionDeclaration> declarations= findFunctionDeclarations(statement, inlineQueryNodes);

            for (SqlCreateFunctionDeclaration decl : declarations)
            {
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
                        //Create proxy table as the function input
                        appendInputTableStatement(builder, tableName, select, decl, writer);
                        //Insert data into proxy table
                        builder.append(this.createInlineQueryFunction(decl, name));
                    }
                } catch (SqlParseException e) {
                    e.printStackTrace();
                }
            }
            //Create view as the function output
            appendCreateViewStatement(builder, name, tableNames, viewNames, statement, declarations);                            
            String sql = builder.toString();
            System.out.println(sql);
            SqlNodeList list = frontend.parseStatements(sql);
            for (SqlNode node : list) {
                result.add(frontend.compile(node.toString(), node, null));
            }
        } catch (SqlParseException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    /**
     * Finds function declarations within a given SQL statement and a list of inline query nodes.
     *
     * @param statement The SQL statement to search within.
     * @param inlineQueryNodes The list of inline query nodes to check against.
     * @return A list of SQL function declarations found within the statement.
     * @throws IllegalArgumentException if the statement is not an instance of SqlSelect.
     */
    private List<SqlCreateFunctionDeclaration> findFunctionDeclarations(SqlNode statement, List<SqlNode> inlineQueryNodes) {
        List<SqlCreateFunctionDeclaration> functionDeclarations = new ArrayList<>();

        // Ensure the statement is of type SqlSelect
        if (!(statement instanceof SqlSelect)) {
            throw new IllegalArgumentException("Statement must be an instance of SqlSelect");
        }

        SqlSelect selectStatement = (SqlSelect) statement;

        // Iterate through each inline query node
        for (SqlNode inlineNode : inlineQueryNodes) {
            // Skip nodes that are not instances of SqlCreateFunctionDeclaration
            if (!(inlineNode instanceof SqlCreateFunctionDeclaration)) {
                continue;
            }

            SqlCreateFunctionDeclaration functionDeclaration = (SqlCreateFunctionDeclaration) inlineNode;

            // Iterate through the select list of the SqlSelect statement
            for (SqlNode selectNode : selectStatement.getSelectList()) {
                // Check if the select node is a function call
                if (selectNode instanceof SqlCall) {
                    SqlCall call = (SqlCall) selectNode;
                    SqlOperator operator = getOperatorFromCall(call);

                    // Add to the list if the operator matches the function declaration name
                    if (operator != null && operator.toString().equals(functionDeclaration.getName().toString())) {
                        functionDeclarations.add(functionDeclaration);
                    }
                }
            }
        }

        return functionDeclarations;
    }

    /**
     * Extracts the operator from a given SQL call. 
     * If the call is an "AS" clause, retrieves the operator from its first operand.
     *
     * @param call The SQL call to extract the operator from.
     * @return The operator of the call, or null if not found.
     */
    private SqlOperator getOperatorFromCall(SqlCall call) {
        SqlOperator operator = call.getOperator();
        
        // Handle "AS" clause by retrieving the operator from the first operand
        if (operator != null && "AS".equals(operator.toString())) {
            List<SqlNode> operands = call.getOperandList();
            if (!operands.isEmpty() && operands.get(0) instanceof SqlCall) {
                return ((SqlCall) operands.get(0)).getOperator();
            }
        }
        
        return operator;
    }

    private void appendInputTableStatement(StringBuilder builder, String tableName, SqlSelect select,
    SqlCreateFunctionDeclaration decl, SqlWriter writer) {
        // Start creating the table with the specified table name
        builder.append("CREATE TABLE ").append(tableName).append(" (");

        // Unparse function parameters to the builder
        decl.getParameters().unparse(writer, 0, 0);

        // Extract parameters from the SELECT statement
        List<String> parameterList = extractParameters(select, decl);

        // Continue with the SELECT statement
        builder.append(") AS\nSELECT DISTINCT ");
        appendParameterList(builder, parameterList);

        // Complete the SQL query by appending the FROM clause
        builder.append(" FROM ").append(select.getFrom().toString()).append(";\n\n");
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
    
        // Iterate over the SELECT list nodes
        for (SqlNode selectNode : select.getSelectList()) {
            if (selectNode instanceof SqlCall) {
                SqlCall call = (SqlCall) selectNode;
                List<SqlNode> operands = call.getOperandList();
                SqlOperator function = call.getOperator();
    
                if (function == null) continue;
    
                // Check if the function is an alias (AS)
                if ("AS".equals(function.toString())) {
                    SqlCall functionCall = (SqlCall) operands.get(0);
                    SqlOperator functionOperator = functionCall.getOperator();
    
                    if (functionOperator != null && functionOperator.toString().equals(decl.getName().toString())) {
                        addOperandsToList(parameterList, functionCall.getOperandList());
                    }
                } else if (function.toString().equals(decl.getName().toString())) {
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

    private void appendCreateViewStatement(StringBuilder builder, SqlIdentifier name, List<String> tableNames, List<String> viewNames,
                                        SqlNode statement, List<SqlCreateFunctionDeclaration> declarations) {
        builder.append("CREATE VIEW ").append(name).append(" AS\n");

        // Iterate over function declarations
        for (int i = 0; i < declarations.size(); i++) {
            SqlCreateFunctionDeclaration decl = declarations.get(i);
            String tableName = tableNames.get(i);
            String viewName = viewNames.get(i);

            if (!isAggregate(decl)) {
                // Append UNION ALL if not the first declaration
                if (i > 0) {
                    builder.append("UNION ALL\n");
                }
                builder.append("SELECT * FROM ").append(viewName).append("\n");
            } else {
                // Handle aggregate functions
                appendAggregateFunction(builder, statement, decl, tableName, i, name);
            }
        }

        // Append FROM clause with table names
        appendFromClause(builder, tableNames);
    }

    private void appendAggregateFunction(StringBuilder builder, SqlNode statement, SqlCreateFunctionDeclaration decl, 
                                        String tableName, int index, SqlIdentifier viewName) {
        if (index == 0) {
            builder.append("SELECT ");
        } else {
            builder.append(", ");
        }

        // Parse the SQL statement to extract parameters
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
                    builder.append(functionParameter).append(" AS ").append(parameterList.get(k)).append("_").append(index + 1);
                }

                builder.append(", (SELECT * FROM ").append(viewName.toString()).append("_").append(decl.getName().toString()).append(")");
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
