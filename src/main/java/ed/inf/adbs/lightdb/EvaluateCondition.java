package ed.inf.adbs.lightdb;

import java.util.Arrays;
import java.util.List;

import net.sf.jsqlparser.expression.BinaryExpression;
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


public class EvaluateCondition extends ExpressionDeParser{
    public List<String> schema;
    public String tuple;
    public boolean value = true;
    public List<String> tables;
    public Expression whereExpression;
    public List<String> tupleItem;

    // constructor for test
    public EvaluateCondition(BinaryExpression expression) {
        this.whereExpression = expression;
    }

    // constructor for SelectOperator
    public EvaluateCondition(List<String> schema, Expression whereExpression, String tuple) {
        this.schema = schema;
        this.whereExpression = whereExpression;
        this.tuple = tuple;
        String[] tupleItem = tuple.split(",");
        // strip 
        for (int i = 0; i < tupleItem.length; i++) {
            tupleItem[i] = tupleItem[i].trim();
        }
        this.tupleItem = Arrays.asList(tupleItem);
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
            String value = tupleItem.get(schema.indexOf(columnName));
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

