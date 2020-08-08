package Launcher;

import java.sql.SQLException;

import ETL.LoadFromLocalToStaging;
import ETL.LoadFromSources;
import ETL.LoadFromStagingToWareHouse;
import configuration.Config;
import dao.ControlDatabase;
import model.ReadFile;
import util.LogUtils;

public class AutoLauncher {
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		Config config = LogUtils.getConfigAutoRun();
		System.out.println("Running warehouse config: " + config.getConfig_name());
		//Chay download
		LoadFromSources LFS = new LoadFromSources();
		LFS.DownLoad(config);
		//Chay extract
		LoadFromLocalToStaging ls = new LoadFromLocalToStaging();
		ls.setConfigName(config.getConfig_name());
		ReadFile rf = new ReadFile();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("control");
		cdb.setTarget_db_name("staging");
		cdb.setTable_name("config");
		rf.setCdb(cdb);
		String file = LogUtils.getFirstFileInLog(config.getConfig_id(), "ER");
		System.out.println("extract file : " + file);
		ls.ExtractToDatabase(rf,file);
		//Chay transform 
		LoadFromStagingToWareHouse LSTW = new LoadFromStagingToWareHouse();
		LSTW.load(config);
	}

}
