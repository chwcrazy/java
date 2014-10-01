package edu.buffalo.cse562;

import java.util.ArrayList;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import edu.buffalo.cse562.Datum.Type;

public class SelectItemsListVistor implements SelectItemVisitor {
	Column[] schema;
	Datum[] tuple;
	Evaluator eval;

	Datum data;
	ArrayList<Datum> AllTableColsDatums;

	public SelectItemsListVistor(Column[] schema, Datum[] tuple) {
		this.schema = schema;
		this.tuple = tuple;
	}

	@Override
	public void visit(AllColumns arg0) {
		// TODO Auto-generated method stub
		int i = 0;
	}

	@Override
	public void visit(AllTableColumns tableCols) {
		// TODO Auto-generated method stub
		String tableName = tableCols.getTable().getName();
		AllTableColsDatums = new ArrayList<Datum>();
		for (Datum dat : tuple) {
			if (dat.colName.contains(tableName)) {
				AllTableColsDatums.add(dat);
			}
		}
	}

	@Override
	public void visit(SelectExpressionItem expItem) {
		// TODO Auto-generated method stub

		data = null;
		Expression exp = expItem.getExpression();
		// if exp contains sum, count,and avg,return
		String expStr = expItem.toString().toLowerCase();
		String newS = new String(expStr);
		newS.replaceAll(" ", "");
		String projectColName;
		if (expItem.getAlias() != null) {

			projectColName = expItem.getAlias();
		} else {
			projectColName = expStr;
		}

		if ((newS.contains("sum(") && newS.contains("("))
				|| (newS.contains("avg(") && newS.contains("("))
				|| (newS.contains("count(") && newS.contains("("))) {

			for (int i = 0; i < tuple.length; i++) {
				if (tuple[i].colName.toLowerCase().equals(
						projectColName.toLowerCase())) {
					data = tuple[i];
					break;
				}
			}
			return;
		}

		// justify whether the exp contain more than one col
		if (containMoreThanOneCol(expStr)) {
			eval = new Evaluator(schema, tuple);
			exp.accept(eval);
			Type type = eval.getType();
			data = getDatums(type, eval.getCmpValue(), projectColName);

			return;
		}

		eval = new Evaluator(schema, tuple);
		exp.accept(eval);
		String expAlias = expItem.getAlias();

		String expName = exp.toString();
		String result = eval.getCmpValue();
		int i = 0;
		for (; i < tuple.length; i++) {
			if (tuple[i].colName.equals(expName)) {
				Datum d = tuple[i];
				if (expAlias != null) {
					data = new Datum.STRING(expAlias, d.value);
				} else {

					data = d;
				}
				break;
			}
		}
		if (i == tuple.length) {
			if (expAlias != null) {
				data = new Datum.STRING(expAlias, result);
			} else {

				data = new Datum.STRING(expName, result);
			}
		}

	}

	public Datum getDatums(Type type, String value, String colN) {
		Datum d = null;
		switch (type) {
		case INT:
			d = new Datum.INT(colN, value);
			break;
		case FLOAT:
			d = new Datum.FLOAT(colN, value);
			break;
		case LONG:
			d = new Datum.LONG(colN, value);
			break;
		case STRING:
			d = new Datum.STRING(colN, value);
			break;
		case DATE:
			d = new Datum.DATE(colN, value);
			break;
		default:
			break;

		}
		return d;

	}

	public boolean containMoreThanOneCol(String s) {
		int count = 0;
		for (Column col : schema) {
			if (s.contains(col.getColumnName().toLowerCase())) {
				count++;
			}
		}
		if (count > 1)
			return true;
		else
			return false;
	}

	// contain at least one col
	public boolean containOneLeastCol(String s) {

		int count = 0;
		for (Column col : schema) {
			if (s.contains(col.getColumnName().toLowerCase())) {
				count++;
			}
		}
		if (count >= 1)
			return true;
		else
			return false;

	}

	public Datum getExpDatum() {
		return data;
	}

	public ArrayList<Datum> getAlltableCols() {
		return AllTableColsDatums;
	}
}
