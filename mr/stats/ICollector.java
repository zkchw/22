package com.transformer.mr.stats;

import java.io.IOException;
import java.sql.PreparedStatement;

import org.apache.hadoop.conf.Configuration;

import com.transformer.dimension.key.BaseDimension;
import com.transformer.dimension.value.BaseStatsValueWritable;
import com.transformer.service.converter.IDimensionConverter;

/**
 * 定义具体kpi对应sql的参数设值
 *
 */
public interface ICollector {
	/**
	 * 定义具体的sql参数设值方法
	 * 
	 * @param conf
	 *            上下文对象
	 * @param key
	 *            输出key
	 * @param value
	 *            输出value
	 * @param pstmt
	 *            数据库设值对象
	 * @param collector
	 *            维度转换类
	 * @throws IOException
	 */
	public void collect(Configuration conf, BaseDimension key, BaseStatsValueWritable value, PreparedStatement pstmt, IDimensionConverter converter) throws IOException;
}
