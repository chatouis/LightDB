package ed.inf.adbs.lightdb;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

public class EvaluateConditionJoin extends ExpressionDeParser {
    Map<String, List<String>> schema;
    String leftTuple;
    String rightTuple;
    boolean value = true;
    List<String> leftTupleItem;
    List<String> rightTupleItem;

    public EvaluateConditionJoin(Map<String, List<String>> schema, Expression whereExpression, String leftTuple, String rightTuple) {
        this.schema = schema;
        this.leftTuple = leftTuple;
        this.rightTuple = rightTuple;
        this.leftTupleItem = tupleToTupleItem(leftTuple);
        this.rightTupleItem = tupleToTupleItem(rightTuple);
    }
    
    private void setValue(boolean value) {
        this.value = value;
    }

    public boolean getValue() {
        return this.value;
    }

    public List<String> tupleToTupleItem(String tuple) {
        String[] tupleItem = tuple.split(",");
        // strip 
        for (int i = 0; i < tupleItem.length; i++) {
            tupleItem[i] = tupleItem[i].trim();
        }
        return Arrays.asList(tupleItem);
    }

    public String findTableOfColumn(String columnName) {
        for (String tableName : schema.keySet()) {
            if (schema.get(tableName).contains(columnName)) {
                return tableName;
            }
        }
        return null;
    }

    public LongValue leftExpressionToLongValue(Expression expression) {
        if (expression instanceof Column) {
            Column column = (Column) expression;
            String columnName = column.getColumnName();
            String tableName = column.getTable().getName();
            String value = leftTupleItem.get(schema.get(tableName).indexOf(columnName));
            return new LongValue(value);
        }
        else if (expression instanceof LongValue) {
            return (LongValue) expression;
        }
        return null;
    }

    public LongValue rightExpressionToLongValue(Expression expression) {
        if (expression instanceof Column) {
            Column column = (Column) expression;
            String columnName = column.getColumnName();
            String tableName = column.getTable().getName();
            String value = rightTupleItem.get(schema.get(tableName).indexOf(columnName));
            return new LongValue(value);
        }
        else if (expression instanceof LongValue) {
            return (LongValue) expression;
        }
        return null;
    }

    @Override
    public void visit (MinorThan minorThan){
        super.visit(minorThan);
        LongValue leftValue = leftExpressionToLongValue(minorThan.getLeftExpression());
        LongValue rightValue = rightExpressionToLongValue(minorThan.getRightExpression());
        setValue(leftValue.getValue() < rightValue.getValue());
    }

    @Override
    public void visit (MinorThanEquals minorThanEquals){
        super.visit(minorThanEquals);
        LongValue leftValue = leftExpressionToLongValue(minorThanEquals.getLeftExpression());
        LongValue rightValue = rightExpressionToLongValue(minorThanEquals.getRightExpression());
        setValue(leftValue.getValue() <= rightValue.getValue());
    }


    @Override
    public void visit (EqualsTo equalsTo){
        super.visit(equalsTo);
        LongValue leftValue = leftExpressionToLongValue(equalsTo.getLeftExpression());
        LongValue rightValue = rightExpressionToLongValue(equalsTo.getRightExpression());
        setValue(leftValue.getValue() == rightValue.getValue());
    }

    @Override
    public void visit (GreaterThan greaterThan){
        super.visit(greaterThan);
        LongValue leftValue = leftExpressionToLongValue(greaterThan.getLeftExpression());
        LongValue rightValue = rightExpressionToLongValue(greaterThan.getRightExpression());
        setValue(leftValue.getValue() > rightValue.getValue());
    }

    @Override
    public void visit (GreaterThanEquals greaterThanEquals){
        super.visit(greaterThanEquals);
        LongValue leftValue = leftExpressionToLongValue(greaterThanEquals.getLeftExpression());
        LongValue rightValue = rightExpressionToLongValue(greaterThanEquals.getRightExpression());
        setValue(leftValue.getValue() >= rightValue.getValue());
    }
    
    @Override
    public void visit (AndExpression andExpression){
        super.visit(andExpression);
        andExpression.getLeftExpression().accept(this);
        boolean leftValue = getValue();
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
