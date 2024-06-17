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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlCreateFunctionDeclaration;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

public class InlineQueryUdfParser extends BaseQueryExtractor {

    private final FunctionBodyParser functionBodyParser;
    private final HashMap<String, String> aliases;

    public InlineQueryUdfParser(SqlIdentifier name, SqlCreateFunctionDeclaration declaration) {
        super(name, declaration);
        this.functionBodyParser = new FunctionBodyParser(name, declaration);
        this.aliases= new HashMap<>();
    }

    public String generateStatements(SqlNode statement) {
        SqlWriter writer = new SqlPrettyWriter(SqlPrettyWriter.config(), builder);

        try {
            SqlParser parser = SqlParser.create(statement.toString().replace("`", ""));
            SqlNode sqlNode = parser.parseQuery();

            if (sqlNode instanceof SqlSelect) {
                SqlSelect select = (SqlSelect) sqlNode;
                if (decl.getParameters().size() == 0) {
                    appendProxyTableStatement(select);
                    builder.append(functionBodyParser.createInlineQueryFunction());
                }
                else{
                    appendProxyTableStatement(select);
                    appendInputTableStatement(select, writer);
                    builder.append(functionBodyParser.createInlineQueryFunction());
                    if (returnsSingleValue()) appendCreateViewStatement(select);
                }
            }
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        return builder.toString();
    }

    private void appendProxyTableStatement(SqlSelect select) {
        builder.append("\nCREATE VIEW ").append(tempTable).append(" AS SELECT * FROM ").append(select.getFrom().toString());
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
     
        builder.append(") AS\nSELECT DISTINCT ");
        appendParameterList(parameterList, viewParam);
    
        builder.append(" FROM ").append(tempTable);
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

    private void appendCreateViewStatement(SqlSelect select) {
        builder.append("CREATE VIEW ").append(finalView).append(" AS\n");
        builder.append("SELECT ").append(extractParameters(select, true).stream().collect(Collectors.joining(", ")).replace("`", ""));
        if (select.getSelectList().size() > 1) builder.append(", ");
        builder.append(alias + "\n");
        builder.append("FROM ").append(tempTable).append("\n");
        builder.append("JOIN ").append(intermediateView).append("\nON ");
        builder.append(appendOnClause(select));
        builder.append(appendHavingClause(select));
        builder.append(";\n");
        
    }
    
    private String appendOnClause(SqlSelect select) {
        StringBuilder builderAlt = new StringBuilder();
        List<String> parameterList = extractParameters(select);
        List<String> viewParam = new ArrayList<String>();
        for (int i = 0; i < functionArguments.size(); i++) {
            String parameter = functionArguments.get(i).toString().split(" ")[0].replace("`", "");
            viewParam.add(parameter);
        }

        for (int i = 0; i < parameterList.size(); i++) {
            if (i > 0) {
                builderAlt.append(" AND ");
            }
            builderAlt.append(tempTable + "." + parameterList.get(i) + " = " + intermediateView + "." + viewParam.get(i));
        }
        return builderAlt.toString();
    }

    private List<String> extractParameters(SqlSelect select) {
        return extractParameters(select, false);
    }

    private String appendHavingClause(SqlSelect select) {
        StringBuilder builderAlt = new StringBuilder();
        if (select.getHaving() != null) {
            SqlNode havingNode = select.getHaving();
            if (!(havingNode instanceof SqlBasicCall)) return "";
            builderAlt.append("\nWHERE ").append(extractOperands(havingNode, null));
            return builderAlt.toString();
        }
        return "";
    }

    /*
     * If fetchAll is true, then all the sql identifiers are extracted from the select list, 
     * i.e from SELECT `AGE`, `PRESENT`, `COUNTUSERBYAGEANDNAME`(`AGE`, `NAME`) AS `PRESENT_COUNT`
     * we extract `AGE`, `PRESENT`, `NAME`
     * 
     * Else, we extract only the parameters of the function, 
     * i.e from SELECT `AGE`, `PRESENT`, `COUNTUSERBYNAME`(`NAME`) AS `PRESENT_COUNT`
     * we extract `NAME`
     *
     */
    private List<String> extractParameters(SqlSelect select, boolean fetchAll) {
        List<String> parameterList = new ArrayList<>();
        for (SqlNode selectNode : select.getSelectList()) {
            if (fetchAll){
                if (selectNode instanceof SqlIdentifier) {
                    parameterList.add(selectNode.toString());
                }
                else if (selectNode instanceof SqlCall) {
                    SqlCall functionCall = (SqlCall) selectNode;
                    SqlOperator functionOperator = functionCall.getOperator();
                    List<SqlNode> functionOperands = functionCall.getOperandList();
                    if (functionOperator != null && functionOperator.toString().equalsIgnoreCase("AS")) {
                        if (!(functionOperands.get(0) instanceof SqlCall)) parameterList.add(functionCall.toString().replace("`", ""));
                    }
                }
                continue;
            }
            
            if (selectNode instanceof SqlCall) {
                SqlCall call = (SqlCall) selectNode;
                List<SqlNode> operands = call.getOperandList();
                SqlOperator function = call.getOperator();
                if (function == null) continue;
                
                if ("AS".equals(function.toString())) {
                    if (operands.get(0) instanceof SqlIdentifier) {
                        aliases.put(operands.get(0).toString(), operands.get(1).toString());
                    }
                    else{
                        SqlCall functionCall = (SqlCall) operands.get(0);
                        SqlOperator functionOperator = functionCall.getOperator();
                        alias = operands.get(1).toString();
                        functionBodyParser.setAlias(alias);
                        if (functionOperator != null && functionOperator.toString().equalsIgnoreCase(decl.getName().toString())) {
                            addOperandsToList(parameterList, functionCall.getOperandList());
                        }
                    }
                } else if (function.toString().equalsIgnoreCase(decl.getName().toString())) {
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