package ed.inf.adbs.lightdb;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;


public class EvaluateCondition extends ExpressionDeParser{
    public Map<String, List<String>> schema;
    public String tuple;
    public boolean value = true;
    public List<String> tables;
    public Expression whereExpression;

    // public EvaluateCondition(List<String> tables, Expression whereExpression, String tuple){
    //     this.tuple = tuple;
    //     this.tables = tables;
    //     this.whereExpression = whereExpression;
    // }

    // public void visit (EqualsTo equalsTo){
    //     String lineData[] = tuple.split(",");
    //     Column left = (Column) equalsTo.getLeftExpression();
    //     Column right = (Column) equalsTo.getRightExpression();
    //     String leftValue = lineData[schema.get(tables).indexOf(left.getColumnName())];
    //     String rightValue = lineData[schema.get(tables).indexOf(right.getColumnName())];
    //     value = value && (leftValue.equals(rightValue)) ;
    // }

    public EvaluateCondition(BinaryExpression expression) {
        this.whereExpression = expression;
    }

    public EvaluateCondition(List<String> tables2, Expression whereExpression2, String tuple2) {
        //TODO Auto-generated constructor stub
    }

    private void setValue(boolean value) {
        this.value = value;
    }

    public boolean getValue() {
        return this.value;
    }

    @Override
    public void visit (MinorThan minorThan){
        super.visit(minorThan);
        String leftString = minorThan.getLeftExpression().toString();
        BigDecimal left = new BigDecimal(leftString);
        String rightString = minorThan.getRightExpression().toString();
        BigDecimal right = new BigDecimal(rightString);
        setValue(left.compareTo(right) < 0);
    }

    @Override
    public void visit (MinorThanEquals minorThanEquals){
        super.visit(minorThanEquals);
        String leftString = minorThanEquals.getLeftExpression().toString();
        BigDecimal left = new BigDecimal(leftString);
        String rightString = minorThanEquals.getRightExpression().toString();
        BigDecimal right = new BigDecimal(rightString);
        setValue(left.compareTo(right) <= 0);
    }

    @Override
    public void visit (EqualsTo equalsTo){
        super.visit(equalsTo);
        String leftString = equalsTo.getLeftExpression().toString();
        BigDecimal left = new BigDecimal(leftString);
        String rightString = equalsTo.getRightExpression().toString();
        BigDecimal right = new BigDecimal(rightString);
        int value = left.compareTo(right);
        setValue(value == 0);
    }

    @Override
    public void visit (GreaterThan greaterThan){
        super.visit(greaterThan);
        String leftString = greaterThan.getLeftExpression().toString();
        BigDecimal left = new BigDecimal(leftString);
        String rightString = greaterThan.getRightExpression().toString();
        BigDecimal right = new BigDecimal(rightString);
        int value = left.compareTo(right);
        setValue(value > 0);
    }

    @Override
    public void visit (GreaterThanEquals greaterThanEquals){
        super.visit(greaterThanEquals);
        String leftString = greaterThanEquals.getLeftExpression().toString();
        BigDecimal left = new BigDecimal(leftString);
        String rightString = greaterThanEquals.getRightExpression().toString();
        BigDecimal right = new BigDecimal(rightString);
        int value = left.compareTo(right);
        setValue(value >= 0);
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

