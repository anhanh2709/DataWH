package Launcher;

import java.sql.SQLException;

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
		//lÃ¢Ì�y ra 1 config coÌ� flag  = 1 Ä‘ÃªÌ‰ chaÌ£y tÆ°Ì£ Ä‘Ã´Ì£ng
		// config tiÃªÌ�p theo seÌƒ tÆ°Ì£ Ä‘Ã´Ì£ng coÌ� flag = 1
		Config config = ConfigUtils.getConfigAutoRun();
		System.out.println("Running warehouse config: " + config.getConfig_name());
		//Chay download
		LoadFromSources LFS = new LoadFromSources();
		// gÄƒÌ�n config cho bÆ°Æ¡Ì�c 1
		LFS.DownLoad(config);
		//Chay extract
		LoadFromLocalToStaging ls = new LoadFromLocalToStaging();
		// gÄƒÌ�n config cho bÆ°Æ¡Ì�c 2
		ls.setConfigName(config.getConfig_name());
		ReadFile rf = new ReadFile();
		ControlDatabase cdb = new ControlDatabase();
		cdb.setConfig_db_name("control");
		cdb.setTarget_db_name("staging");
		cdb.setTable_name("config");
		rf.setCdb(cdb);
		// lÃ¢Ì�y file Ä‘Ã¢Ì€u tiÃªn trong log maÌ€ coÌ� config_id = configid cuÌ‰a config mÆ¡Ì�i lÃ¢Ì�y ra vaÌ€ coÌ� trang thaÌ�i laÌ€ ER
		String file = LogUtils.getFirstFileInLog(config.getConfig_id(), "ER");
		System.out.println("extract file : " + file);
		ls.ExtractToDatabase(rf,file);
		//Chay transform 
		LoadFromStagingToWareHouse LSTW = new LoadFromStagingToWareHouse();
		// gÄƒÌ�n config cho bÆ°Æ¡Ì�c 3
		LSTW.load(config);
	}

}
