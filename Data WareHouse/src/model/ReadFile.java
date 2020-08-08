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

//	Đọc từng dòng dữ liệu
	private String readLines(String value, String delim) {
		String values = "";

//		tạo ra một lớp StringTokenizer dựa trên chuỗi chỉ định và dấu phân cách.
		StringTokenizer stoken = new StringTokenizer(value, delim);
<<<<<<< HEAD
//		countTokens: Trả về tổng số lượng của các stoken.
=======
//		countTokens: Trả về tổng số lượng của các token.
>>>>>>> d30a133289db428aa684aedaf38dbedf6f1a5adf
		int countToken = stoken.countTokens();
		String lines = "(";

		for (int j = 0; j < countToken; j++) {
//			next token: Trả về token tiếp theo khi duyệt đối tượng StringTokenizer
			String token = stoken.nextToken();
<<<<<<< HEAD
//			nếu là cuối cùng thì ) + | không thì là dấu ,
			lines += (j == countToken - 1) ? "'" + token.trim() + "')|" : "'" + token.trim() + "',";
=======

			lines += (j == countToken - 1) ? "'" + token.trim() + "')|" : "'" + token.trim() + "',";

>>>>>>> d30a133289db428aa684aedaf38dbedf6f1a5adf
			values += lines;
			lines = "";
		}
		return values;
	}

<<<<<<< HEAD
//	đọc txt
=======
>>>>>>> d30a133289db428aa684aedaf38dbedf6f1a5adf
	public String readValuesTXT(File s_file, int count_field) {
		if (!s_file.exists()) {
			return null;
		}
		String values = "";
		String delim = "|";
		try {
<<<<<<< HEAD
//			Đọc một dòng dữ liệu có trong file:
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file), "utf8")); //mở file để đọc
			String line = bReader.readLine();
//			nếu delim là "\t" thì trả về \t
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
//			Kiểm tra có header hay không
//			nếu là số thì đọc còn k phải số thì bỏ qua
=======
			// Đọc một dòng dữ liệu có trong file:
//			BufferedReader đọc văn bản từ inputStream dựa trên các kí tự 
//			readline đọc từng dòng
//			FileInputStream đọc file từ các byte từ file input
//			InputStreamReader đọc kí tự
			BufferedReader bReader = new BufferedReader(new InputStreamReader(new FileInputStream(s_file), "utf8"));
			String line = bReader.readLine();
//			indexOf trả về kí tự đã cho là "\t" 
			if (line.indexOf("\t") != -1) {
				delim = "\t";
			}
			// Kiểm tra xem tổng số field trong file có đúng format hay không
			// (11 trường)
//			if (new StringTokenizer(line, delim).countTokens() != (count_field + 1)) {
//				bReader.close();
//				return null;
//			}
//			 STT|Mã sinh viên|Họ lót|Tên|...-> line.split(delim)[0]="STT"
//			 không phải số nên là header -> bỏ qua line
//			 Kiểm tra xem có phần header hay không
//			tạo một matcher khớp với đầu vào đã cho với mẫu.
>>>>>>> d30a133289db428aa684aedaf38dbedf6f1a5adf
			if (Pattern.matches(NUMBER_REGEX, line.split(delim)[0])) {
				values += readLines(line + delim, delim);
			}
//			chạy dòng while ghi hết dữ liệu từng dòng đọc được lại vào values
			while ((line = bReader.readLine()) != null) {
				values += readLines(line + " " + delim, delim);
			}
			bReader.close();
<<<<<<< HEAD
//			trả giá trị từ đầu tới cuối
=======
//			subString in ra giá trị từ đầu tới cuối
>>>>>>> d30a133289db428aa684aedaf38dbedf6f1a5adf
			return values.substring(0, values.length() - 1);
		} catch (NoSuchElementException | IOException e) {
			e.printStackTrace();
			return null;
		}
	}

<<<<<<< HEAD
//	đọc xlsx
	public String readValuesXLSX(File s_file, int countField) {
		String values = ""; //dữ liệu cuối cùng có được
		String value = ""; //dữ liệu đọc từng dòng
		String delim = "|";
		try {
			FileInputStream fileIn = new FileInputStream(s_file); //file truyền vào
			XSSFWorkbook workBook = new XSSFWorkbook(fileIn); // file xlsx
			XSSFSheet sheet = workBook.getSheetAt(0); //1 sheet trong file xlsx
			Iterator<Row> rows = sheet.iterator(); // lấy từng hàng trong 1 sheet
//			Kiểm tra xem có phần header hay không, nếu không có phần header
//			Gọi rows.next, nếu có header thì vị trí dòng dữ liệu là 1.
//			Nếu kiểm tra mà không có header thì phải set lại cái row bắt đầu
//			ở vị trí 0, hổng ấy là bị sót dữ liệu dòng 1 nha.
=======
	public String readValuesXLSX(File s_file, int countField) {
		String values = "";
		String value = "";
		String delim = "|";
		try {
			FileInputStream fileIn = new FileInputStream(s_file);
			XSSFWorkbook workBook = new XSSFWorkbook(fileIn); //file xlsx
			XSSFSheet sheet = workBook.getSheetAt(0);
			Iterator<Row> rows = sheet.iterator();
			// Kiểm tra xem có phần header hay không, nếu không có phần header
			// Gọi rows.next, nếu có header thì vị trí dòng dữ liệu là 1.
			// Nếu kiểm tra mà không có header thì phải set lại cái row bắt đầu
			// ở vị trí 0, hổng ấy là bị sót dữ liệu dòng 1 nha.
>>>>>>> d30a133289db428aa684aedaf38dbedf6f1a5adf
			if (rows.next().cellIterator().next().getCellType().equals(CellType.NUMERIC)) {
				// iterator lấy danh sách
				rows = sheet.iterator();
			}
			while (rows.hasNext()) { // co lay phan tu tiep theo khong
<<<<<<< HEAD
				Row row = rows.next(); // lay phan tu tiep theo
//				 Kiểm tra coi cái số trường ở trong file excel có đúng với
//				 số trường có trong cái bảng mình tạo sẵn ở trong table
//				 staging không
				if (row.getLastCellNum() < countField + 1 || row.getLastCellNum() > countField + 2) {
					workBook.close();
					return null;
				}

//				Bắt đầu lấy giá trị trong các ô ra:
//				Iterator<Cell> cells = row.cellIterator();
				for (int cn = 0; cn < countField; cn++) {
//					tạo ô trống
=======
				Row row = rows.next(); //lay phan tu tiep theo
				// Kiểm tra coi cái số trường ở trong file excel có đúng với
				// số trường có trong cái bảng mình tạo sẵn ở trong table
				// staging không
//				if (row.getLastCellNum() < countField + 1 || row.getLastCellNum() > countField + 2) {
//					workBook.close();
//					return null;
//				}
				// Bắt đầu lấy giá trị trong các ô ra:
//				Iterator<Cell> cells = row.cellIterator();
				for (int cn = 0; cn < countField; cn++) {
					// Cell cell = cells.next();
>>>>>>> d30a133289db428aa684aedaf38dbedf6f1a5adf
					Cell cell = row.getCell(cn, Row.MissingCellPolicy.CREATE_NULL_AS_BLANK);
//					ép kiểu cho ô trống
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
//						công thức trong excel
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
//						giá trị trống
						value += " " + delim;
						break;
					default:
//						dòng bị trống thì bỏ qua
						if (cn < 2) {
							value += (long) cell.getNumericCellValue() + delim;
						} else
//							giá trị trống
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
