// package ed.inf.adbs.lightdb;

// import java.io.IOException;

// import net.sf.jsqlparser.expression.Expression;
// import net.sf.jsqlparser.statement.select.PlainSelect;
// import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

// public class SelectOperator extends ScanOperator{
//     public SelectOperator(String inputFile) throws IOException {
//         super(inputFile);
//     }

//     public String getNextTuple(Expression where) throws IOException {
//         String line = super.getNextTuple();
//         ExpressionDeParser deparser = new ExpressionDeParser(where) {
//             @Override
//             public void visit(EqualsTo equalsTo) {
//                 if (equalsTo.getLeftExpression().toString().equals("A")) {
//                     equalsTo.setLeftExpression("B");
//                 }
//             }
//         }
//         where.accept(deParser);
//         return line;
//     }

//     public void reset() throws IOException {
//         super.reset();
//     }
// }



