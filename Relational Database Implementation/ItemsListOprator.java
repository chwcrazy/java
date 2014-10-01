package edu.buffalo.cse562;

import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitor;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.Datum.Type;

public class ItemsListOprator implements ItemsListVisitor {
	Datum[] tuple;
	Column[] schema;
	public String value;
	public Type type;

	public ItemsListOprator(Datum[] tuple, Column[] schema) {
		this.tuple = tuple;
		this.schema = schema;

	}

	@Override
	public void visit(SubSelect arg0) {
		// TODO Auto-generated method stub
		value = "";
	}

	@Override
	public void visit(ExpressionList expList) {
		// TODO Auto-generated method stub
		List<Expression> listOfExp = expList.getExpressions();
		for (int i = 0; i < listOfExp.size(); i++) {
			Expression exp = listOfExp.get(i);
			Evaluator eval = new Evaluator(schema, tuple);
			exp.accept(eval);
			value = eval.getCmpValue();
			type = eval.getType();
		}
	}

	public String getValue() {
		return value;
	}

	public Type getType() {
		return type;
	}

}
