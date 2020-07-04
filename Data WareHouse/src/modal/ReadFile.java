package modal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
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
		if (stoken.countTokens() > 0) {
			stoken.nextToken();
		}
		int countToken = stoken.countTokens();
		String lines = "(";
		for (int j = 0; j < countToken; j++) {
			String token = stoken.nextToken();
			if (Pattern.matches(NUMBER_REGEX, token)) {
				lines += (j == countToken - 1) ? token.trim() + ")," : token.trim() + ",";
			} else {
				lines += (j == countToken - 1) ? "'" + token.trim() + "')," : "'" + token.trim() + "',";
			}
			values += lines;
			lines = "";
		}
		return values;
	}

	public String readValuesTXT(File s_file, String delim) {
		String values = "";
		try {
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file)));
			String line;
			while ((line = bReader.readLine()) != null) {
				System.out.println(line);
				values += readLines(line, delim);
			}
			bReader.close();
			System.out.println(values);
			return values.substring(0, values.length() - 1);
			

		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

	public String readValuesXLSX(File s_file) {
		String values = "";
		String value = "";
		try {
			FileInputStream fileIn = new FileInputStream(s_file);
			XSSFWorkbook workBooks = new XSSFWorkbook(fileIn);
			XSSFSheet sheet = workBooks.getSheetAt(0);
			Iterator<Row> rows = sheet.iterator();
			rows.next();
			while (rows.hasNext()) {
				Row row = rows.next();
				Iterator<Cell> cells = row.cellIterator();
				while (cells.hasNext()) {
					Cell cell = cells.next();
					CellType cellType = cell.getCellType();
					switch (cellType) {
					case NUMERIC:
						if (DateUtil.isCellDateFormatted(cell)) {
							SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
							value += dateFormat.format(cell.getDateCellValue()) + "|";
						} else {
							value += (long) cell.getNumericCellValue() + "|";
						}

						break;
					case STRING:
						value += cell.getStringCellValue() + "|";
						break;
					default:
						break;
					}
				}
				values += readLines(value.substring(0, value.length()), "|");
				value = "";
			}
			workBooks.close();
			fileIn.close();
			return values.substring(0, values.length()-1);
		} catch (IOException e) {
			return null;
		}
	}

	public boolean writeDataToBD(String column_list, String target_table, String values) throws ClassNotFoundException {
		if (cdb.insertValues(column_list, values, target_table))
			return true;
		return false;
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
