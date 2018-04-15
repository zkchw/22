package com.transformer.mr.stats.nu;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.transformer.common.KpiType;
import com.transformer.dimension.key.stats.StatsUserDimension;
import com.transformer.dimension.value.MapWritableValue;

/**
 * 统计对应维度的uuid的数量<br/>
 * 因为相同的uuid只计算一次，所以存在数据的去重操作，解决方案：
 *  1. 使用set集合进行数据去重
 *  2. ??
 *
 */
public class NewInstallUserReducer extends Reducer<StatsUserDimension, Text, StatsUserDimension, MapWritableValue> {
	private MapWritableValue outputValue = new MapWritableValue();
	
	@Override
	protected void reduce(StatsUserDimension key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		// 1. 创建一个集合，用于数据的去重，并统计数量
		Set<String> unique = new HashSet<String>();
		
		// 2. 循环变量value，将数据保存到集合中
		for (Text value : values) {
			unique.add(value.toString());
		}
		
		// 3. 获取去重后uuid的数量
		int newInstallUsers = unique.size();
		
		// 4. 构建输出对象
		MapWritable map = this.outputValue.getValue();
		map.put(new IntWritable(-1), new IntWritable(newInstallUsers));
		KpiType kpi = KpiType.valueOfName(key.getStatsCommon().getKpi().getKpiName());
		this.outputValue.setKpi(kpi);
		
		// 5. 构建输出对象
		context.write(key, this.outputValue);
	}
}
