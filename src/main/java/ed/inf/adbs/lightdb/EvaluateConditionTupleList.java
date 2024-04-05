package ed.inf.adbs.lightdb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;


public class EvaluateConditionTupleList extends ExpressionDeParser{
    public List<String> tupleList;
    public boolean value = true;
    public Expression whereExpression;
    public List<List<String>> tupleItemList;
    public List<List<String>> schemaList;

    // constructor for test
    public EvaluateConditionTupleList(Expression expression) {
        this.whereExpression = expression;
    }

    // constructor for SelectOperator
    public EvaluateConditionTupleList(List<List<String>> schemaList, Expression whereExpression, List<String> tupleList) {
        this.schemaList = schemaList;
        this.whereExpression = whereExpression;
        this.tupleList = tupleList;
        this.tupleItemList = new ArrayList<List<String>>();
        for (String tuple : tupleList) {
            this.tupleItemList.add(tupleToTupleItem(tuple));
        }

    }

    public List<String> tupleToTupleItem(String tuple) {
        String[] tupleItem = tuple.split(",");
        // strip 
        for (int i = 0; i < tupleItem.length; i++) {
            tupleItem[i] = tupleItem[i].trim();
        }
        return Arrays.asList(tupleItem);
    }

    private void setValue(boolean value) {
        this.value = value;
    }

    public boolean getValue() {
        return this.value;
    }

    public LongValue childExpressionToLongValue(Expression expression) {
        if (expression instanceof Column) {
            Column column = (Column) expression;
            String columnName = column.getColumnName();
            for (List<String> schema : schemaList) {
                // todo : support using alias and tableName at the same time
                if (schema.contains(columnName) == true) {
                    //  search for correct table
                    int indexOfTable= schemaList.indexOf(schema);
                    int indexOfColoumn = schema.indexOf(columnName);
                    String value = tupleItemList.get(indexOfTable).get(indexOfColoumn);
                    return new LongValue(value);
                }
                else if (schema.contains(column.getTable().toString() + "." + columnName) == true) {
                    // consider alias
                    int indexOfTable= schemaList.indexOf(schema);
                    int indexOfColoumn = schema.indexOf(column.getTable().toString() + "." + columnName);
                    String value = tupleItemList.get(indexOfTable).get(indexOfColoumn);
                    return new LongValue(value);
                }
            }
        }
        else if (expression instanceof LongValue) {
            return (LongValue) expression;
        }
        return null;
    }

    @Override
    public void visit (MinorThan minorThan){
        super.visit(minorThan);
        LongValue leftValue = childExpressionToLongValue(minorThan.getLeftExpression());
        LongValue rightValue = childExpressionToLongValue(minorThan.getRightExpression());
        setValue(leftValue.getValue() < rightValue.getValue());
    }

    @Override
    public void visit (MinorThanEquals minorThanEquals){
        super.visit(minorThanEquals);
        LongValue leftValue = childExpressionToLongValue(minorThanEquals.getLeftExpression());
        LongValue rightValue = childExpressionToLongValue(minorThanEquals.getRightExpression());
        setValue(leftValue.getValue() <= rightValue.getValue());
    }

    @Override
    public void visit (EqualsTo equalsTo){
        super.visit(equalsTo);
        LongValue leftValue = childExpressionToLongValue(equalsTo.getLeftExpression());
        LongValue rightValue = childExpressionToLongValue(equalsTo.getRightExpression());
        setValue(leftValue.getValue() == rightValue.getValue());
    }

    @Override
    public void visit (NotEqualsTo notEqualsTo){
        super.visit(notEqualsTo);
        LongValue leftValue = childExpressionToLongValue(notEqualsTo.getLeftExpression());
        LongValue rightValue = childExpressionToLongValue(notEqualsTo.getRightExpression());
        setValue(leftValue.getValue() != rightValue.getValue());
    }

    @Override
    public void visit (GreaterThan greaterThan){
        super.visit(greaterThan);
        LongValue leftValue = childExpressionToLongValue(greaterThan.getLeftExpression());
        LongValue rightValue = childExpressionToLongValue(greaterThan.getRightExpression());
        setValue(leftValue.getValue() > rightValue.getValue());
    }

    @Override
    public void visit (GreaterThanEquals greaterThanEquals){
        super.visit(greaterThanEquals);
        LongValue leftValue = childExpressionToLongValue(greaterThanEquals.getLeftExpression());
        LongValue rightValue = childExpressionToLongValue(greaterThanEquals.getRightExpression());
        setValue(leftValue.getValue() >= rightValue.getValue());
    }
    
    @Override
    public void visit (AndExpression andExpression){
        super.visit(andExpression);
        andExpression.getLeftExpression().accept(this);
        boolean leftValue = getValue();
        if (leftValue == false) {
            setValue(false);
            return;
        }
        andExpression.getRightExpression().accept(this);
        boolean rightValue = getValue();
        setValue(leftValue && rightValue);
    }

    @Override
    public void visit (OrExpression orExpression){
        super.visit(orExpression);
        orExpression.getLeftExpression().accept(this);
        boolean leftValue = getValue();
        orExpression.getRightExpression().accept(this);
        boolean rightValue = getValue();
        setValue(leftValue || rightValue);
    }


}

