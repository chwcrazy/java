package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import edu.buffalo.cse562.Datum.Type;

public class ProjectOperator implements SelectItemVisitor {
	Column[] schema;
	Datum[] tuple;
	List<SelectItem> selectItemList;
	// Evaluator eval;
	SelectItemsListVistor selItemOpr;
	Datum selectedDatum;
	ArrayList<Type> tupleTypeL;

	public ArrayList<String> selectNameList;

	public ArrayList<Datum> outPutDatums;

	public ProjectOperator(Column[] schema, Datum[] tuple,
			List<SelectItem> selectItemList) {
		this.schema = schema;
		this.tuple = tuple;
		// eval = new Evaluator(schema, tuple);
		selItemOpr = new SelectItemsListVistor(schema, tuple);

		this.selectItemList = selectItemList;
		outPutDatums = new ArrayList<Datum>();
		tupleTypeL = new ArrayList<>();

	}

	public void projectOneTuple() {

		for (int i = 0; i < selectItemList.size(); i++) {
			SelectItem selItem = selectItemList.get(i);
			selItem.accept(this);
			// if (selectedDatum != null) {
			// outPutDatums.add(selectedDatum);
			// }
		}
	}

	@Override
	public void visit(AllColumns arg0) {
		// TODO Auto-generated method stub
		int i = 0;
	}

	@Override
	public void visit(AllTableColumns tableCols) {
		// TODO Auto-generated method stub
		SelectItemsListVistor sltItemVistor = new SelectItemsListVistor(schema,
				tuple);
		tableCols.accept(sltItemVistor);
		ArrayList<Datum> TableColsDatums = sltItemVistor.getAlltableCols();
		if (!TableColsDatums.isEmpty()) {
			outPutDatums.addAll(TableColsDatums);
		}
	}

	@Override
	public void visit(SelectExpressionItem selectItem) {
		// TODO Auto-generated method stub
		selectItem.accept(selItemOpr);
		selectedDatum = selItemOpr.getExpDatum();
		if (selectedDatum != null) {
			outPutDatums.add(selectedDatum);
			tupleTypeL.add(selectedDatum.type);

		}

	}
}
