package edu.buffalo.cse562;

public abstract class Datum implements Comparable<Datum> {
	public static enum Type {
		INT, LONG, FLOAT, STRING, DATE
	};

	public Type type;
	public String value;
	public String colName;

	public Datum() {
	}

	public abstract int cmpTo(Datum data);

	// public static class LONG extends Datum {
	// public static long dataV;
	// public LONG(String s) {
	// dataV = java.lang.Long.parseLong(s);
	// type = Type.INT;
	// }
	//
	// public boolean
	// }
	//

	public static int stringToInt(String s) {
		return Integer.parseInt(s);
	}

	public static long stringToLONG(String s) {
		return Long.parseLong(s);
	}

	public static float stringToFlt(String s) {
		return Float.parseFloat(s);
	}

	public static class INT extends Datum {
		// public int dataV;
		public INT(String colName, String s) {
			// dataV = java.lang.Integer.parseInt(s);
			this.colName = colName;
			value = s;
			type = Type.INT;
		}

		public INT(String colName, int s) {
			this.colName = colName;
			value = String.valueOf(s);
			type = Type.INT;
		}

		public int cmpTo(Datum data) {
			switch (data.type) {
			case INT:
				int value_int_this = stringToInt(value);
				int value_int_data = stringToInt(data.value);
				return value_int_this == value_int_data ? 0
						: value_int_this < value_int_data ? -1 : 1;
			case LONG:
				long value_long_this = stringToLONG(value);
				long value_long_data = stringToLONG(data.value);
				return value_long_this == value_long_data ? 0
						: value_long_this < value_long_data ? -1 : 1;
			default:
				break;
			}
			return 0;
		}
	}

	public static class LONG extends Datum {
		// public int dataV;
		public LONG(String colName, String s) {
			this.colName = colName;
			value = s;
			type = Type.LONG;
		}

		public LONG(String colName, long s) {
			this.colName = colName;
			value = String.valueOf(s);
			type = Type.LONG;
		}

		public int cmpTo(Datum data) {
			long value_this = stringToLONG(value);
			long value_data = stringToLONG(data.value);
			return value_this == value_data ? 0 : value_this < value_data ? -1
					: 1;
		}
	}

	public static class FLOAT extends Datum {
		// public float dataV;
		public FLOAT(String colNmae, String s) {
			// dataV = java.lang.Float.parseFloat(s);
			this.colName = colNmae;
			value = s;
			type = Type.FLOAT;
		}

		public FLOAT(String colName, float s) {
			// dataV = java.lang.Float.parseFloat(s);
			this.colName = colName;
			value = String.valueOf(s);
			type = Type.FLOAT;
		}

		public int cmpTo(Datum data) {
			double value_this = stringToFlt(value);
			double value_data = stringToFlt(data.value);
			return value_this == value_data ? 0 : value_this < value_data ? -1
					: 1;
		}
	}

	public static class STRING extends Datum {
		// public String dataV;
		public STRING(String colName, String s) {
			// dataV = s;
			this.colName = colName;
			value = s;
			type = Type.STRING;
		}

		public int cmpTo(Datum data) {

			return value.compareTo(data.value);

		}
	}

	public static class DATE extends Datum {
		// public String dataV;
		public DATE(String colName, String s) {
			// dataV = s;
			this.colName = colName;
			value = s;
			type = Type.DATE;
		}

		public int cmpTo(Datum data) {
			String[] dateArr1 = value.split("-");
			String[] dateArr2 = data.value.split("-");
			for (int i = 0; i < dateArr1.length; i++) {
				int date1Int = stringToInt(dateArr1[i]);
				int date2Int = stringToInt(dateArr2[i]);

				if (date1Int == date2Int)
					continue;
				else {
					return date1Int < date2Int ? -1 : 1;
				}

			}
			return 0;
		}
	}

	@Override
	public int compareTo(Datum a) {
		// TODO Auto-generated method stub

		return cmpTo(a);
	}

	public String toString() {

		return value;
	}

	public int hashCode() {
		return value.hashCode();
	}
	
	public void setColName(String colN){
		colName = colN;
	}
}