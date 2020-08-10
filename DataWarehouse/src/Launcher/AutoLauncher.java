package Launcher;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import ETL.LoadFromLocalToStaging;
import ETL.LoadFromSources;
import ETL.LoadFromStagingToWareHouse;
import configuration.Config;
import dao.ControlDatabase;
import model.ReadFile;
import util.ConfigUtils;
import util.LogUtils;

public class AutoLauncher {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// Lấy 1 config ra để chạy tự đông, config có flag = 1;
		// config tiếp theo sẽ đc chuyển flag thành 1 flag vừa lấy ra sẽ chuyển thành 0 
		Config config = ConfigUtils.getConfigAutoRun();
		System.out.println("Running warehouse config: " + config.getConfig_name());
		//Chay download
		LoadFromSources LFS = new LoadFromSources();
		// 
		LFS.DownLoad(config);
		List<String> fileNames = new ArrayList<String>();
		fileNames = LogUtils.getFilesInLog(config.getConfig_id(), "ER");
		//Chay extract
		LoadFromLocalToStaging ls = new LoadFromLocalToStaging();
		// gắn config cho bước 2;
		ls.setConfigName(config.getConfig_name());
		ReadFile rf = new ReadFile();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("control");
		cdb.setTarget_db_name("staging");
		cdb.setTable_name("config");
		rf.setCdb(cdb);
		// chạy từng file trong danh sách file lấy ra từ log
		for (String file : fileNames) {
			System.out.println("extract file : " + file);
			ls.ExtractToDatabase(rf,file);
			//Chay transform 
			LoadFromStagingToWareHouse LSTW = new LoadFromStagingToWareHouse();
			// gắn config cho bước 3;
			LSTW.load(config);
		}
		
	}

}
