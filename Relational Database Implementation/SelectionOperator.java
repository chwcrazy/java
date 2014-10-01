package edu.buffalo.cse562;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;

public class SelectionOperator implements Operator {

	Operator input;
	Column[] schema;
	Expression condition;

	public SelectionOperator(Operator input, Column[] schema,
			Expression condition) {
		this.input = input;
		this.schema = schema;
		this.condition = condition;
	}

	@Override
	public Datum[] readOneTuple() {
		// TODO Auto-generated method stub
		Datum[] tuple = null;
		while (tuple == null) {
			tuple = input.readOneTuple();
			if (tuple == null)
				return null;
			Evaluator eval = new Evaluator(schema, tuple);
			condition.accept(eval);

			if (!eval.getBool()) {
				tuple = null;
			}
		}

		return tuple;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		input.reset();
	}

}
