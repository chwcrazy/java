package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.OrderByVisitor;
import edu.buffalo.cse562.Datum.Type;

public class OrderByElementVistor implements OrderByVisitor {

	Datum[] o1;
	Datum[] o2;
	int cmpInt;

	public OrderByElementVistor(Datum[] o1, Datum[] o2) {
		this.o1 = o1;
		this.o2 = o2;
	}

	@Override
	public void visit(OrderByElement orderEle) {
		// TODO Auto-generated method stub
		boolean isAsc = orderEle.isAsc();
		Expression expr = orderEle.getExpression();
		Evaluator eval_o1 = new Evaluator(null, o1);
		expr.accept(eval_o1);
		String colValue_o1 = eval_o1.getCmpValue();
		Type type_o1 = eval_o1.getType();
		Evaluator eval_o2 = new Evaluator(null, o2);
		expr.accept(eval_o2);
		String colValue_o2 = eval_o2.getCmpValue();
		Type type_o2 = eval_o2.getType();

		if (isAsc) {
			if (type_o1 == Type.STRING || type_o1 == Type.DATE) {
				cmpInt = colValue_o1.compareTo(colValue_o2);
			} else {
				cmpInt = Double.valueOf(colValue_o1).compareTo(
						Double.valueOf(colValue_o2));
			}
		} else {
			if (type_o1 == Type.STRING || type_o1 == Type.DATE) {
				cmpInt = colValue_o2.compareTo(colValue_o1);
			} else {
				cmpInt = Double.valueOf(colValue_o2).compareTo(
						Double.valueOf(colValue_o1));
			}
		}

	}

	public int getCmpInt() {
		return cmpInt;
	}

}
