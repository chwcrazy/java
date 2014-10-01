package edu.buffalo.cse562;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SubSelect;
import edu.buffalo.cse562.Datum.Type;

public class Evaluator implements ExpressionVisitor {
	Datum[] tuple;
	Column[] schema;
	boolean BoolValue;
	// public String cmpColName; // the compare column
	public String cmpDataValue; // the data value of compare column in
								// expression
								// public String cmpType;
	// int[] cmpIndex; // index the compare is >(1),=(0), <(-1),
	// >=(1,0),<=(-1,0)

	public Function func;
	public String selectItemIndex;
	public Type colType;

	public Evaluator(Column[] schema, Datum[] tuple) {
		this.tuple = tuple;
		this.schema = schema;
		BoolValue = false;
	}

	@Override
	public void visit(NullValue arg0) {
		// TODO Auto-generated method stub

		BoolValue = true;
	}

	@Override
	public void visit(Function func) {
		// TODO Auto-generated method stub
		if (func.isAllColumns()) {
			cmpDataValue = "*";
			colType = Type.STRING;
		} else {
			ExpressionList expList = func.getParameters();
			ItemsListOprator oper = new ItemsListOprator(tuple, schema);
			expList.accept(oper);
			cmpDataValue = oper.getValue();
			colType = oper.getType();
		}

		String funName = func.getName().toLowerCase();
		switch (funName) {

		// case "count":
		// cmpDataValue = "1";
		// break;
		case "date":
			colType = Type.DATE;

		default:
			break;

		}

		this.func = func;
		selectItemIndex = "Function";
	}

	@Override
	public void visit(InverseExpression arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(JdbcParameter arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(DoubleValue arg0) {
		// TODO Auto-generated method stub

		cmpDataValue = String.valueOf(arg0);
	}

	@Override
	public void visit(LongValue longV) {
		// TODO Auto-generated method stub
		cmpDataValue = String.valueOf(longV);
	}

	@Override
	public void visit(DateValue arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(TimeValue arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(TimestampValue arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(Parenthesis parenth) {
		// TODO Auto-generated method stub

		Expression exp = parenth.getExpression();

		exp.accept(this);
	}

	@Override
	public void visit(StringValue arg0) {
		// TODO Auto-generated method stub
		cmpDataValue = arg0.toString();
	}

	@Override
	public void visit(Addition addExp) {
		// TODO Auto-generated method stub
		addExp.getLeftExpression().accept(this);
		String leftValue = this.getCmpValue();
		addExp.getRightExpression().accept(this);
		String rightValue = this.getCmpValue();
		if (colType == Type.INT || colType == Type.LONG) {
			cmpDataValue = String.valueOf(Long.valueOf(leftValue)
					+ Long.valueOf(rightValue));
		} else {

			cmpDataValue = String.valueOf(Double.valueOf(leftValue)
					+ Double.valueOf(rightValue));
		}
	}

	@Override
	public void visit(Division divExp) {
		// TODO Auto-generated method stub
		divExp.getLeftExpression().accept(this);
		String leftValue = this.getCmpValue();
		divExp.getRightExpression().accept(this);
		String rightValue = this.getCmpValue();
		cmpDataValue = String.valueOf(Double.valueOf(leftValue)
				/ Double.valueOf(rightValue));
	}

	@Override
	public void visit(Multiplication mulExp) {
		// TODO Auto-generated method stub
		mulExp.getLeftExpression().accept(this);
		String leftValue = this.getCmpValue();
		mulExp.getRightExpression().accept(this);
		String rightValue = this.getCmpValue();
		if (colType == Type.INT || colType == Type.LONG) {
			cmpDataValue = String.valueOf(Long.valueOf(leftValue)
					* Long.valueOf(rightValue));
		} else {
			cmpDataValue = String.valueOf(Double.valueOf(leftValue)
					* Double.valueOf(rightValue));
		}
	}

	@Override
	public void visit(Subtraction subExp) {
		// TODO Auto-generated method stub
		Expression leftExp = subExp.getLeftExpression();
		leftExp.accept(this);
		String leftValue = getCmpValue();
		Expression rightExp = subExp.getRightExpression();
		rightExp.accept(this);
		String rightValue = getCmpValue();
		if (colType == Type.INT || colType == Type.LONG) {
			cmpDataValue = String.valueOf(Long.valueOf(leftValue)
					- Long.valueOf(rightValue));
		} else {

			cmpDataValue = String.valueOf(Double.valueOf(leftValue)
					- Double.valueOf(rightValue));
		}

	}

	@Override
	public void visit(AndExpression andExp) {
		// TODO Auto-generated method stub
		Expression left_exp = andExp.getLeftExpression();
		left_exp.accept(this);
		boolean leftValue = this.getBool();
		if (leftValue == false) {
			BoolValue = false;
		} else {
			Expression right_exp = andExp.getRightExpression();
			right_exp.accept(this);
			boolean rightValue = this.getBool();
			BoolValue = leftValue && rightValue;
		}
	}

	@Override
	public void visit(OrExpression orExp) {
		// TODO Auto-generated method stub
		Expression left_exp = orExp.getLeftExpression();
		left_exp.accept(this);
		boolean leftValue = this.getBool();
		if (leftValue == true) {
			BoolValue = true;
		} else {
			Expression right_exp = orExp.getRightExpression();
			right_exp.accept(this);
			boolean rightValue = this.getBool();
			BoolValue = leftValue || rightValue;
		}
	}

	@Override
	public void visit(Between btExp) {
		// TODO Auto-generated method stub
		Expression left_exp = btExp.getBetweenExpressionStart();
		left_exp.accept(this);
		boolean leftValue = this.getBool();
		if (leftValue == false) {
			BoolValue = false;
		} else {
			Expression right_exp = btExp.getBetweenExpressionEnd();
			right_exp.accept(this);
			boolean rightValue = this.getBool();
			BoolValue = leftValue && rightValue;
		}
	}

	@Override
	public void visit(EqualsTo eqlExp) {
		// TODO Auto-generated method stub
		Expression leftExp = eqlExp.getLeftExpression();
		leftExp.accept(this);
		String leftValue = this.getCmpValue();
		Expression rightExp = eqlExp.getRightExpression();
		rightExp.accept(this);
		String rightValue = this.getCmpValue();
		if (rightValue.contains("'")) {
			rightValue = rightValue.replaceAll("'", "");
		}

		int cmpResult = 0;
		if (colType == Type.DATE || colType == Type.STRING) {
			cmpResult = leftValue.compareTo(rightValue);
		} else {
			cmpResult = Double.valueOf(leftValue).compareTo(
					Double.valueOf((rightValue)));
		}

		if (cmpResult == 0) {
			BoolValue = true;
		} else {
			BoolValue = false;
		}

	}

	@Override
	public void visit(GreaterThan greaterExp) {
		// TODO Auto-generated method stub
		// cmpIndex = new int[]{1,0};
		//
		Expression leftExp = greaterExp.getLeftExpression();
		leftExp.accept(this);
		String leftValue = this.getCmpValue();
		Expression rightExp = greaterExp.getRightExpression();
		rightExp.accept(this);
		String rightValue = this.getCmpValue();
		if (rightValue.contains("'")) {
			rightValue = rightValue.replaceAll("'", "");
		}
		int cmpResult = 0;
		if (colType == Type.DATE || colType == Type.STRING) {
			cmpResult = leftValue.compareTo(rightValue);
		} else {
			cmpResult = Double.valueOf(leftValue).compareTo(
					Double.valueOf((rightValue)));
		}

		if (cmpResult > 0) {
			BoolValue = true;
		} else {
			BoolValue = false;
		}

	}

	@Override
	public void visit(GreaterThanEquals greaterEqualExp) {
		// TODO Auto-generated method stub
		// cmpIndex = new int[]{1,0};
		//
		Expression leftExp = greaterEqualExp.getLeftExpression();
		leftExp.accept(this);
		String leftValue = this.getCmpValue();
		Expression rightExp = greaterEqualExp.getRightExpression();
		rightExp.accept(this);
		String rightValue = this.getCmpValue();
		if (rightValue.contains("'")) {
			rightValue = rightValue.replaceAll("'", "");
		}

		int cmpResult = 0;
		if (colType == Type.DATE || colType == Type.STRING) {
			cmpResult = leftValue.compareTo(rightValue);
		} else {
			cmpResult = Double.valueOf(leftValue).compareTo(
					Double.valueOf((rightValue)));
		}
		// int cmpResult = 0;
		// switch (colType) {
		// case INT:
		// cmpResult = Integer.valueOf(leftValue).compareTo(
		// Integer.valueOf((rightValue)));
		// break;
		// case Double:
		// cmpResult = Double.valueOf(leftValue).compareTo(
		// Double.valueOf((rightValue)));
		// break;
		//
		// case LONG:
		// cmpResult = Long.valueOf(leftValue).compareTo(
		// Long.valueOf((rightValue)));
		// break;
		// case STRING:
		// cmpResult = leftValue.compareTo(rightValue);
		// break;
		// default:
		// break;
		// }
		if (cmpResult >= 0) {
			BoolValue = true;
		} else {
			BoolValue = false;
		}

	}

	@Override
	public void visit(InExpression arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(IsNullExpression arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(LikeExpression exp) {
		// TODO Auto-generated method stub
		Expression leftExp = exp.getLeftExpression();
		leftExp.accept(this);
		String leftValue = this.getCmpValue();
		Expression rightExp = exp.getRightExpression();
		rightExp.accept(this);
		String rightValue = this.getCmpValue();
		int lastInd = rightValue.lastIndexOf("'");
		rightValue = rightValue.substring(1, lastInd);
		rightValue = rightValue.replaceAll("%", ".*");
		rightValue = rightValue.replaceAll("_", ".{1}");

		BoolValue = leftValue.matches(rightValue);
	}

	@Override
	public void visit(MinorThan minorThanExp) {
		// TODO Auto-generated method stub
		Expression leftExp = minorThanExp.getLeftExpression();
		leftExp.accept(this);
		String leftValue = this.getCmpValue();
		Expression rightExp = minorThanExp.getRightExpression();
		rightExp.accept(this);
		String rightValue = this.getCmpValue();
		if (rightValue.contains("'")) {
			rightValue = rightValue.replaceAll("'", "");
		}

		int cmpResult = 0;
		if (colType == Type.DATE || colType == Type.STRING) {
			cmpResult = leftValue.compareTo(rightValue);
		} else {
			cmpResult = Double.valueOf(leftValue).compareTo(
					Double.valueOf((rightValue)));
		}
		if (cmpResult < 0) {
			BoolValue = true;
		} else {
			BoolValue = false;
		}
	}

	@Override
	public void visit(MinorThanEquals minorEqualExp) {
		// TODO Auto-generated method stub
		Expression leftExp = minorEqualExp.getLeftExpression();
		leftExp.accept(this);
		String leftValue = this.getCmpValue();
		Expression rightExp = minorEqualExp.getRightExpression();
		rightExp.accept(this);
		String rightValue = this.getCmpValue();
		if (rightValue.contains("'")) {
			rightValue = rightValue.replaceAll("'", "");
		}

		int cmpResult = 0;
		if (colType == Type.DATE || colType == Type.STRING) {
			cmpResult = leftValue.compareTo(rightValue);
		} else {
			cmpResult = Double.valueOf(leftValue).compareTo(
					Double.valueOf((rightValue)));
		}
		if (cmpResult <= 0) {
			BoolValue = true;
		} else {
			BoolValue = false;
		}
	}

	@Override
	public void visit(NotEqualsTo notEqlExp) {
		// TODO Auto-generated method stub
		Expression leftExp = notEqlExp.getLeftExpression();
		leftExp.accept(this);
		String leftValue = this.getCmpValue();
		Expression rightExp = notEqlExp.getRightExpression();
		rightExp.accept(this);
		String rightValue = this.getCmpValue();
		if (rightValue.contains("'")) {
			rightValue = rightValue.replaceAll("'", "");
		}

		int cmpResult = 0;
		if (colType == Type.DATE || colType == Type.STRING) {
			cmpResult = leftValue.compareTo(rightValue);
		} else {
			cmpResult = Double.valueOf(leftValue).compareTo(
					Double.valueOf((rightValue)));
		}

		if (cmpResult != 0) {
			BoolValue = true;
		} else {
			BoolValue = false;
		}

	}

	@Override
	public void visit(Column col) {
		// TODO Auto-generated method stub
		String colName = null;
		if (col.getTable().getName() != null) { // from item is only one table.
												// if the number of tables is
												// bigger than 2,
			// the table name in the col will be
			// null, we need to plus the table
			// name(or alias) before the column
			// name(the attribute name
			colName = col.getTable().getName() + "." + col.getColumnName();

		} else {
			colName = col.getColumnName();
		}

		for (Datum d : tuple) {
			String d_col;
			if (!colName.contains(".") && d.colName.contains(".")) {
				d_col = (d.colName.split("\\."))[1];
			} else if (colName.contains(".") && (!d.colName.contains("."))) {
				d_col = null;
			} else {
				d_col = d.colName;
			}
			if (colName.toLowerCase().equals(d_col)) {
				cmpDataValue = d.value;
				colType = d.type;
				break;
			}
		}
		selectItemIndex = "Column";

	}

	@Override
	public void visit(SubSelect subS) {
		// TODO Auto-generated method stub
		// arg0.getSelectBody();

		SelectBodyVistor selBodyVistor = new SelectBodyVistor(Main.dataDir,
				Main.tables);
		subS.getSelectBody().accept(selBodyVistor);

		ArrayList<ArrayList<Datum>> outP = selBodyVistor.outPutArrList;
		Datum d = outP.get(0).get(0);
		cmpDataValue = d.value;
		colType = d.type;
	}

	@Override
	public void visit(CaseExpression arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(WhenClause arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(ExistsExpression arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(AllComparisonExpression arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(AnyComparisonExpression arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(Concat arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(Matches arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(BitwiseAnd arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(BitwiseOr arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	@Override
	public void visit(BitwiseXor arg0) {
		// TODO Auto-generated method stub
		BoolValue = true;
	}

	public boolean getBool() {

		return BoolValue;
	}

	public String getCmpValue() {
		return cmpDataValue;
	}

	public Type getType() {
		return colType;
	}

	// public int dateCompare(String date1, String date2){
	// String[] ymds1 = date1.split("-");
	// String[] ymds2 = date2.split("-");
	//
	// if(ymds1[0])
	// }

}
