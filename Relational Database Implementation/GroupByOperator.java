package edu.buffalo.cse562;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitor;
import edu.buffalo.cse562.Datum.DATE;
import edu.buffalo.cse562.Datum.Type;

public class GroupByOperator implements SelectItemVisitor {

	List<SelectItem> selectItemList;
	Datum[] tuple;
	List groupList;
	Column[] schema;
	Evaluator eval;

	ArrayList<Datum> aggregateDatums; // the datums of aggregation of select
										// statement
	String groupByStr;
	Datum aggDatum;
	Double sum;
	int countNum;
	int funcIndex; // Index of functions
	int avgFuncIndex; // Index of average functions
	boolean isAvgFunc; // the index of whether the function is average function
	ArrayList<Double> sumL; // the list of sum of average functions for one
							// group.
	ArrayList<Integer> countNumL; // //the list of average functions' number of
									// one group.
	ArrayList<String> distValues;
	ArrayList<ArrayList<String>> distinctValueL;

	public HashMap<String, ArrayList<Datum>> dataAfterGroup;

	public HashMap<String, ArrayList<Integer>> countNumHashMap;
	public HashMap<String, ArrayList<Double>> sumHashMap;
	public HashMap<String, ArrayList<ArrayList<String>>> distinctHashMap;

	public GroupByOperator(List<SelectItem> selectItemList, Datum[] tuple,
			List groupList, Column[] schema,
			HashMap<String, ArrayList<Datum>> dataAfterGroup,
			HashMap<String, ArrayList<Double>> sumHashMap,
			HashMap<String, ArrayList<Integer>> countNumHashMap,
			HashMap<String, ArrayList<ArrayList<String>>> distinctHashMap) {
		this.selectItemList = selectItemList;
		this.tuple = tuple;
		this.groupList = groupList;
		this.schema = schema;
		this.dataAfterGroup = dataAfterGroup;
		this.sumHashMap = sumHashMap;
		this.countNumHashMap = countNumHashMap;
		this.distinctHashMap = distinctHashMap;
		// eval = new Evaluator(schema, tuple);
		funcIndex = 0;
		avgFuncIndex = 0;
	}

	public void groupOneTuple() {
		isAvgFunc = false;
		// if(!(tuple[3].value.equals("Brand#12")&&
		// tuple[4].value.equals("PROMO ANODIZED STEEL") &&
		// tuple[5].value.equals("46"))){
		// return;
		// }
		groupByStr = grpListToStr();

		aggregateDatums = new ArrayList<Datum>();
		sumL = new ArrayList<Double>();
		countNumL = new ArrayList<Integer>();
		distinctValueL = new ArrayList<ArrayList<String>>();

		for (int m = 0; m < selectItemList.size(); m++) {
			SelectItem item = selectItemList.get(m);
			item.accept(this);
			if (aggDatum != null) {
				aggregateDatums.add(aggDatum);
				distinctValueL.add(distValues);
				if (isAvgFunc) {
					sumL.add(sum);
					countNumL.add(countNum);
				}
			}
		}
		distinctHashMap.put(groupByStr, distinctValueL);
		dataAfterGroup.put(groupByStr, aggregateDatums);
		sumHashMap.put(groupByStr, sumL);
		countNumHashMap.put(groupByStr, countNumL);
	}

	@Override
	public void visit(AllColumns arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(AllTableColumns arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void visit(SelectExpressionItem selectItem) {
		// TODO Auto-generated method stub
		aggDatum = null;
		String sltItemAlias = selectItem.getAlias();
		Expression exp = selectItem.getExpression();
		eval = new Evaluator(schema, tuple);
		exp.accept(eval);
		if (eval.selectItemIndex.equals("Function")) {
			Function func = eval.func;

			String funName = func.getName().toLowerCase();
			if (sltItemAlias == null) {
				sltItemAlias = func.toString();
			}
			// List<Expression> expList = func.getParameters().getExpressions();
			ArrayList<Datum> valueList = dataAfterGroup.get(groupByStr);
			ArrayList<ArrayList<String>> distVL = distinctHashMap
					.get(groupByStr);
			if (valueList != null) {
				aggDatum = valueList.get(funcIndex);
				distValues = distVL.get(funcIndex);
			}

			switch (funName) {
			case "count":
				if (aggDatum == null) {
					aggDatum = new Datum.INT(sltItemAlias, 1);
					distValues = new ArrayList<>();
					distValues.add(eval.cmpDataValue);
				} else {
					if (func.isDistinct()) {
						if (!(distValues.contains(eval.cmpDataValue))) {
							distValues.add(eval.cmpDataValue);
							String newValue = String.valueOf(Integer
									.valueOf(aggDatum.value) + 1);
							aggDatum.value = newValue;
						}
					} else {

						String newValue = String.valueOf(Integer
								.valueOf(aggDatum.value) + 1);
						aggDatum.value = newValue;
					}
				}
				break;
			case "min":
				if (aggDatum == null) {
					aggDatum = creatNewDatum(sltItemAlias, eval.getCmpValue(),
							eval.colType);
				} else {
					int cmpInt = aggDatum.value.compareTo(eval.getCmpValue());
					if (cmpInt > 0) {
						String newValue = eval.getCmpValue();
						aggDatum.value = newValue;
					}
				}
				break;

			case "sum":
				if (aggDatum == null) {
//					if(eval.colType == null){
//						int i = 0;
//						i++;
//						
//					}
					aggDatum = creatNewDatum(sltItemAlias, eval.getCmpValue(),
							eval.colType);
					
				} else {
					String newValue = null;
					switch (eval.colType) {
					case INT:
						newValue = String.valueOf(Integer
								.valueOf(aggDatum.value)
								+ Integer.valueOf(eval.getCmpValue()));
						break;

					case FLOAT:
						newValue = String.valueOf(Double
								.valueOf(aggDatum.value)
								+ Double.valueOf(eval.getCmpValue()));
						break;
					case LONG:
						newValue = String.valueOf(Long.valueOf(aggDatum.value)
								+ Long.valueOf(eval.getCmpValue()));
						break;
					default:
						break;
					}
					aggDatum.value = newValue;
				}
				break;
			case "avg":
				isAvgFunc = true;
				if (aggDatum == null) {
					aggDatum = creatNewDatum(sltItemAlias, eval.getCmpValue(),
							eval.colType);
					sum = Double.valueOf(eval.getCmpValue());
					countNum = 1;
				} else {
					sum = sumHashMap.get(groupByStr).get(avgFuncIndex)
							+ Double.valueOf(eval.getCmpValue());
					countNum = countNumHashMap.get(groupByStr)
							.get(avgFuncIndex) + 1;
					String newValue = String.valueOf(sum / countNum);
					aggDatum.value = newValue;
				}
				avgFuncIndex++;
				break;
			default:
				break;
			}
			funcIndex++;
		}
	}

	public Datum creatNewDatum(String name, String value, Type type) {
		Datum newDatum = null;
		switch (type) {
		case INT:
			newDatum = new Datum.INT(name, value);
			break;
		case LONG:
			newDatum = new Datum.LONG(name, value);
			break;
		case FLOAT:
			newDatum = new Datum.FLOAT(name, value);
			break;
		case STRING:
			newDatum = new Datum.STRING(name, value);
			break;
		case DATE:
			newDatum = new Datum.DATE(name, value);
			break;
		default:
			break;
		}
		return newDatum;
	}

	public String grpListToStr() {
		StringBuffer groupByColDatums = new StringBuffer(); // the datums of
															// columns in group
															// by
		// statementgroupByColDatums
		if (groupList == null)
			return null;
		for (int i = 0; i < groupList.size(); i++) {
			if (groupList.get(i) instanceof Column) {
				Column groupByCol = (Column) groupList.get(i);

				String colName;
				if (groupByCol.getTable().getName() != null) {

					colName = groupByCol.getTable().getName() + "."
							+ groupByCol.getColumnName();

				} else {
					colName = groupByCol.getColumnName();
				}

				for (int j = 0; j < tuple.length; j++) {
					Datum tupleDatum = tuple[j];
					if (!colName.contains(".")
							&& tupleDatum.colName.contains(".")
							&& tupleDatum.colName.contains(colName)) {
						colName = tupleDatum.colName.split("\\.")[0] + "."
								+ colName;
					}
					if (tupleDatum.colName.equals(colName)) {
						groupByColDatums.append(tupleDatum.colName);
						groupByColDatums.append("^");
						groupByColDatums.append(tupleDatum.type);
						groupByColDatums.append("^");
						groupByColDatums.append(tupleDatum.value);
						groupByColDatums.append("%");
					}
				}
			} else {
				// columnIndex
			}
		}

		return groupByColDatums.toString();
	}

	public static Datum[] groupStrToDatums(String s) {
		if (s == null) {
			return new DATE[0];
		}

		String[] strs = s.split("%");
		Datum[] datums = new Datum[strs.length];
		for (int i = 0; i < strs.length; i++) {
			String str = strs[i];
			String[] datumValStr = str.split("\\^");

			switch (datumValStr[1]) {
			case "INT":
				datums[i] = new Datum.INT(datumValStr[0], datumValStr[2]);
				datums[i].type = Type.INT;
				break;
			case "FLOAT":
				datums[i] = new Datum.FLOAT(datumValStr[0], datumValStr[2]);
				datums[i].type = Type.FLOAT;
				break;
			case "LONG":
				datums[i] = new Datum.LONG(datumValStr[0], datumValStr[2]);
				datums[i].type = Type.LONG;
				break;
			case "STRING":
				datums[i] = new Datum.STRING(datumValStr[0], datumValStr[2]);
				datums[i].type = Type.STRING;
				break;
			case "DATE":
				datums[i] = new Datum.DATE(datumValStr[0], datumValStr[2]);
				datums[i].type = Type.DATE;
				break;
			default:
				break;
			}
		}

		return datums;
	}
}
