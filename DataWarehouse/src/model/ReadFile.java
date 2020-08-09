package model;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import dao.ControlDatabase;

public class ReadFile {
	static final String NUMBER_REGEX = "^[0-9]+$";
	static final String DATE_FORMAT = "yyyy-MM-dd";
	private ControlDatabase cdb;
	private String config_db_name;
	private String target_db_name;
	private String table_name;

	public ReadFile() {
		cdb = new ControlDatabase(this.config_db_name, this.table_name, this.target_db_name);
	}

	private String readLines(String value, String delim) {
		String values = "";

		StringTokenizer stoken = new StringTokenizer(value, delim);
		int countToken = stoken.countTokens();
		String lines = "(";
//		duyệt hết số stoken cắt ra 
		for (int j = 0; j < countToken; j++) {
			String token = stoken.nextToken();
//			nếu là cuối cùng thì ) + | không thì là dấu ,
			lines += (j == countToken - 1) ? "'" + token.trim() + "')|" : "'" + token.trim() + "',";
			values += lines;
			lines = "";
		}
		return values;
	}

	public String readValuesTXT(File s_file, int count_field) {
		if (!s_file.exists()) {
			return null;
		}
		String values = "";
		String delim = "|";
		try {
//			Đọc một dòng dữ liệu có trong file:
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file), "utf8"));
			String line = bReader.readLine();
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
//			 không phải số nên là header -> bỏ qua line
//			 Kiểm tra xem có phần header hay không

			if (Pattern.matches(NUMBER_REGEX, line.split(delim)[0])) {
				values += readLines(line + delim, delim);
			}
//			đọc dữ liệu và xử lý null
			while ((line = bReader.readLine()) != null) {
				values += readLines(line + " " + delim, delim);
			}
			bReader.close();
//			subString cắt ra giá trị từ đầu tới cuối
			return values.substring(0, values.length() - 1);
		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String readValuesXLSX(File s_file, int countField) {
		String values = ""; // dữ liệu cuối cùng có được
		String value = "";  // dữ liệu từng dòng
		String delim = "|";
		try {
			FileInputStream fileIn = new FileInputStream(s_file);
			XSSFWorkbook workBook = new XSSFWorkbook(fileIn); // file xlsx
			XSSFSheet sheet = workBook.getSheetAt(0);
			Iterator<Row> rows = sheet.iterator();
//			Kiểm tra xem có header hay không
			if (rows.next().cellIterator().next().getCellType().equals(CellType.NUMERIC)) {
//				lấy danh sách nhiều dòng rows
				rows = sheet.iterator();
			}
		
			while (rows.hasNext()) { 
				Row row = rows.next();
//				Bắt đầu lấy giá trị trong các ô ra:
				for (int cn = 0; cn < countField; cn++) {
//					Lấy ô ra và xữ lí ô trống
					Cell cell = row.getCell(cn, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
//					lấy kiểu dữ liệu của ô ra để đọc
					CellType cellType = cell.getCellType();
					switch (cellType) {
					case NUMERIC:
						if (DateUtil.isCellDateFormatted(cell)) {
							SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT);
							value += dateFormat.format(cell.getDateCellValue()) + delim;
						} else {
							value += (long) cell.getNumericCellValue() + delim;
						}
						break;
					case STRING:
						value += cell.getStringCellValue() + delim;
						break;
					case FORMULA:
						switch (cell.getCachedFormulaResultType()) {
						case NUMERIC:
							value += (long) cell.getNumericCellValue() + delim;
							break;
						case STRING:
							value += cell.getStringCellValue() + delim;
							break;
						default:
							value += " " + delim;
							break;
						}
						break;
					case BLANK:
						value += " " + delim;
						break;
					default:
//						dòng bị trống thì 2 ô đầu là số điền vào 2 số 0
						if (cn < 2) {
							value += (long) cell.getNumericCellValue() + delim;
						} else
							value += " " + delim;
						break;
					}
				}
//				nếu đọc đủ số trường rồi thì hàng dữ liệu đó + với dấu |
				if (row.getLastCellNum() == countField + 1) {
					value += "|";
				}
				values += readLines(value, delim);
				value = "";
			}
//			đọc xong thì đóng lại
			workBook.close();
			fileIn.close();
//			trả về dữ liệu từ đầu đến cuối
			return values.substring(0, values.length() - 1);
		} catch (Exception e) {
			return null;
		}
	}

	public int writeDataToBD(String column_list, String target_table, String values) throws ClassNotFoundException {
		int stagingCount = 0;
		if ((stagingCount  = cdb.insertValues(column_list, values, target_table)) != -1)
			return stagingCount;
		return -1;
	}

	public void setConfig_db_name(String config_db_name) {
		this.config_db_name = config_db_name;
	}

	public void setTarget_db_name(String target_db_name) {
		this.target_db_name = target_db_name;
	}

	public String getTable_name() {
		return table_name;
	}

	public void setTable_name(String table_name) {
		this.table_name = table_name;
	}

	public ControlDatabase getCdb() {
		return cdb;
	}

	public void setCdb(ControlDatabase cdb) {
		this.cdb = cdb;
	}

}
