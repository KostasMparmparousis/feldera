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
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.dbsp.sqlCompiler.compiler.frontend.calciteCompiler.SqlCreateFunctionDeclaration;

import java.util.List;

public abstract class BaseQueryExtractor {
    protected StringBuilder builder;
    protected SqlCreateFunctionDeclaration decl;
    protected String tempTable;
    protected String tableWithFunctionArguments;
    protected String intermediateView;
    protected String finalView;
    protected SqlNodeList functionArguments;

    protected BaseQueryExtractor(SqlIdentifier name, SqlCreateFunctionDeclaration declaration) {
        this.builder = new StringBuilder();
        decl = declaration;
        functionArguments = decl.getParameters();
        finalView = name.toString();
        tableWithFunctionArguments = finalView + "_" + decl.getName().toString() + "_INPUT";
        intermediateView = finalView + "_" + decl.getName().toString();
        tempTable = finalView + "_TEMP";
    }

    /*
     * Parse through a conditional expression
     * (i.e. `PERSON`.`AGE` = `USERAGE` AND `PERSON`.`PRESENT` = TRUE)
     * and extract the operands
     * (i.e. `PERSON`.`AGE`, `USERAGE`, `PERSON`.`PRESENT`, TRUE)
     */
    protected String extractOperands(SqlNode node) {
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

    // Determines if the function declaration is an aggregate function based on the return type and the function body
    protected boolean isAggregate() {
        
        return decl.getReturnType() != null;
    }

    protected void appendWhereClause(SqlNode whereNode) {
        if (whereNode == null || !(whereNode instanceof SqlBasicCall)) {
            return;
        }
        String whereClause = " WHERE " + extractOperands(whereNode).replace("`", "");
        builder.append(whereClause);
    }
}
