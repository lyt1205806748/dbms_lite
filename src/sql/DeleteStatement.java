package sql;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import bean.WhereType;
import create.TableRead;
import utils.RandomAccessFileRW;
import utils.Table;
import utils.User;

/**
 * 按照条件删除表--> 本质上是将表的flag置为false DELETE FROM project WHERE id>11
 * 
 * @author 卅TAT
 */
public class DeleteStatement {
	private String FilePath = "D:\\Code_Java\\database"; // 数据库主路径

	public User processInsert(String statement, User u1) {
		String state = statement.toLowerCase().replace(";", "");
		String[] where = state.split("where");
		String tbName = null;
		if (where[0].split("from").length > 1) {
			tbName = where[0].split("from")[1].trim();

		} else { 
			u1.setSelectResult("null");
			u1.setLogs("Error: 1064 - You have an error in your SQL syntax~logs:" + u1.toString()
					+ "     \n>>operation failure");
			return u1;
		}

		TableRead tableRead = new TableRead();
		Table fv = tableRead.getFV(tbName, u1);
		ArrayList<Integer> queryIndex = new WhereType().getType(statement, fv);// 下标从0开始
		System.out.println(queryIndex);

		String filePath = FilePath + "\\" + u1.getUseDB() + "\\" + tbName + ".dbf";
		File f = new File(filePath);
		RandomAccessFile raf = null;
		try {
			raf = new RandomAccessFile(f, "rws");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		RandomAccessFileRW rafrw = new RandomAccessFileRW();

		if (f.length() == 0) {
			u1.setLogs("Error：file is empty");
			return u1;
		}

		for (Integer integer : queryIndex) {
			try {
				rafrw.writeBooleanRAF(raf, integer*128, false);
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			raf.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		u1.setLogs("OK~Logs: " + u1.toString() + "  \n>>delete table " + tbName + " success");
		return u1;
	}
}
