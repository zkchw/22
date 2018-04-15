package com.transformer.mr.stats.nu;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;

import com.transformer.common.GlobalConstants;
import com.transformer.dimension.key.BaseDimension;
import com.transformer.dimension.key.stats.StatsUserDimension;
import com.transformer.dimension.value.BaseStatsValueWritable;
import com.transformer.dimension.value.MapWritableValue;
import com.transformer.mr.stats.ICollector;
import com.transformer.service.converter.IDimensionConverter;

public class StatsDeviceBrowserNewInstallUserCollector implements ICollector {

	@Override
	public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt,
			IDimensionConverter converter) throws IOException {
		// 1. 数据类型强转
		StatsUserDimension statsUserDimension = (StatsUserDimension) key;
		MapWritableValue mapWritableValue = (MapWritableValue) value;

		// 2. 获取new install users
		int newInstallUsers = ((IntWritable) mapWritableValue.getValue().get(new IntWritable(-1))).get();

		// 3.设置对应sql的参数
		/**
		 * INSERT INTO `stats_device_browser`(
			`platform_dimension_id`,
			`date_dimension_id`,
			`browser_dimension_id`,
			`new_install_users`,
			`created`)
			VALUES(?, ?, ?, ?, ?) 
			ON DUPLICATE KEY UPDATE `new_install_users` = ?
		 */
		int i = 0;
		try {
			pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getPlatform()));
			pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getStatsCommon().getDate()));
			pstmt.setInt(++i, converter.getDimensionIdByValue(statsUserDimension.getBrowser()));
			pstmt.setInt(++i, newInstallUsers);
			pstmt.setString(++i, conf.get(GlobalConstants.RUNNING_DATE_PARAMES));
			pstmt.setInt(++i, newInstallUsers);

			// 添加到批量处理中
			pstmt.addBatch();
		} catch (SQLException e) {
			throw new IOException(e);
		}
	}

}
