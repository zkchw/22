package com.transformer.mr.stats.nu;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.transformer.common.EventLogConstants;
import com.transformer.common.EventLogConstants.EventEnum;
import com.transformer.common.GlobalConstants;
import com.transformer.dimension.key.stats.StatsUserDimension;
import com.transformer.dimension.value.MapWritableValue;
import com.transformer.mr.stats.TransformerOutputFormat;
import com.transformer.util.TimeUtil;

/**
 * 计算new install user相关kpi的入口执行类
 *
 */
public class NewInstallUsersRunner implements Tool {
	private Configuration conf;

	/**
	 * main方法，执行入口
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		test1(args);
	}

	/**
	 * 正常运行
	 * 
	 * @param args
	 */
	private static void test1(String[] args) {
		try {
			// 调用执行
			int exitCode = ToolRunner.run(new NewInstallUsersRunner(), args);
			if (exitCode == 0) {
				System.out.println("运行成功");
			} else {
				System.out.println("运行失败");
			}
		} catch (Exception e) {
			System.err.println("job运行异常" + e);
		}
	}

	@Override
	public void setConf(Configuration that) {
		// 添加配置文件，最好在HBaseConfiguration.create调用前添加
		// 配置jdbc连接相关属性
		that.addResource("transformer-env.xml");
		// 配置kpi和sql直接的映射关系
		that.addResource("query-mapping.xml");
		// 配置kpi和collector直接的映射关系
		that.addResource("output-collector.xml");

		this.conf = HBaseConfiguration.create(that);
	}

	@Override
	public Configuration getConf() {
		return this.conf;
	}

	@Override
	public int run(String[] args) throws Exception {
		// 1. 获取上下文配置
		Configuration conf = this.getConf();

		// 2. 设置参数
		this.processArgs(conf, args);

		// 3. 创建job对象
		Job job = Job.getInstance(conf, "new_install_users");

		// 4. 设置job的相关参数
		job.setJarByClass(NewInstallUsersRunner.class);

		// 5. 设置InputFormat
		// 6. 设置Mapper
		// 由于从hbase读取数据，5和6需要进行合并
		this.setHBaseInputConfig(job);
		// 7. 设置Reducer
		job.setReducerClass(NewInstallUserReducer.class);
		job.setOutputKeyClass(StatsUserDimension.class);
		job.setOutputValueClass(MapWritableValue.class);
		// 8. 设置OutputFormat
		// TODO: 结果输出到hdfs, 默认outputFormamt使用toString方法进行数据输出
//		TextOutputFormat.setOutputPath(job, new Path("/frank/nu/" + System.currentTimeMillis()));
		job.setOutputFormatClass(TransformerOutputFormat.class);

		// 9. job执行, 执行成功返回0，失败返回1
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * 处理参数，一般处理时间参数
	 * 
	 * @param conf
	 * @param args
	 */
	private void processArgs(Configuration conf, String[] args) {
		String date = null;
		for (int i = 0; i < args.length; i++) {
			if ("-d".equals(args[i])) {
				if (i + 1 < args.length) {
					date = args[++i];
					break;
				}
			}
		}

		// 查看是否需要默认参数
		if (StringUtils.isBlank(date) || !TimeUtil.isValidateRunningDate(date)) {
			date = TimeUtil.getYesterday(); // 默认时间是昨天
		}
		// 保存到上下文中间
		conf.set(GlobalConstants.RUNNING_DATE_PARAMES, date);
	}

	/**
	 * 设置从hbase读取数据的相关代码
	 * 
	 * @param job
	 * @throws IOException
	 */
	private void setHBaseInputConfig(Job job) throws IOException {
		// 获取job的上下文
		Configuration conf = job.getConfiguration();
		// 获取给定的参数，是执行那天的数据
		String dateStr = conf.get(GlobalConstants.RUNNING_DATE_PARAMES);

		// 构建scan应用的filter
		FilterList filterList = new FilterList();
		// 1. 构建只获取需要字段的filter
		String[] columns = new String[] { // SingleColumnValueFilter会在列名过滤之后进行过滤
				EventLogConstants.LOG_COLUMN_NAME_PLATFORM, // 平台名称
				EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME, // 浏览器名称
				EventLogConstants.LOG_COLUMN_NAME_VERSION, // 版本
				EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION, // 浏览器版本
				EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME, // 服务器器时间
				EventLogConstants.LOG_COLUMN_NAME_UUID, // uuid
				EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME// 事件名称， 不能删除
		};
		filterList.addFilter(this.getColumnFilter(columns));
		// 2. 构建filter过滤，非launch事件的数据
		filterList.addFilter(new SingleColumnValueFilter(
				// 应用过根据column对应的value值进行过滤， 如果hbase对应表中没有给定的列，那么数据不过滤，全部返回
				EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME, // 给定的family
				Bytes.toBytes(EventLogConstants.LOG_COLUMN_NAME_EVENT_NAME), // 给定的列名称
				CompareOp.EQUAL, // 比较符号，相等
				Bytes.toBytes(EventEnum.LAUNCH.alias) // 比较的value
		));

		// 由于我们需要获取天、周、月的数据进行统计，所以需要获取多张表的数据
		long date = TimeUtil.parseString2Long(dateStr);
		long startDate, endDate; // 指定开始时间和结束时间[startDate,endDate)

		long startWeek = TimeUtil.getFirstDayOfThisWeek(date);
		long startMonth = TimeUtil.getFirstDayOfThisMonth(date);

		long endWeek = TimeUtil.getFirstDayOfNextWeek(date);
		long endMonth = TimeUtil.getFirstDayOfNextMonth(date);

		long endOfToDay = TimeUtil.getTodayInMillis() + GlobalConstants.DAY_OF_MILLISECONDS;

		// startDate可定是startWeek和startMonth直接最早的一个
		startDate = Math.min(startWeek, startMonth);

		// endDate是endWeek和endMonth最大的一个，但是时间不能够超过endOfToDay
		if (endOfToDay > endWeek || endOfToDay > endMonth) {
			endDate = Math.max(endWeek, endMonth);
		} else {
			endDate = endOfToDay;
		}

		// 创建HBaseAdmin进行表是否存在进行判断
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(conf);
		} catch (Exception e) {
			throw new RuntimeException("创建hbaseadmin对象失败", e);
		}

		// 构建scan的集合
		List<Scan> scans = new ArrayList<Scan>();
		try {
			for (long begin = startDate; begin < endDate;) {
				// 构建表名称的后缀
				String tableNameSuffix = TimeUtil.parseLong2String(begin, TimeUtil.HBASE_TABLE_NAME_SUFFIX_FORMAT);
				// 构建表名称
				String tableName = EventLogConstants.HBASE_NAME_EVENT_LOGS + tableNameSuffix;

				if (admin.tableExists(tableName)) {
					// 表存在，进行scan的构建
					// 构建Scan进行添加
					Scan scan = new Scan();
					scan.setFilter(filterList);
					// 要求表存在, 设置表名称
					scan.setAttribute(Scan.SCAN_ATTRIBUTES_TABLE_NAME, Bytes.toBytes(tableName));
					scans.add(scan);
				}

				// 时间累加
				begin += GlobalConstants.DAY_OF_MILLISECONDS;
			}
		} finally {
			try {
				// 关闭连接
				admin.close();
			} catch (Exception e) {
				// nothings
			}
		}

		// 过滤
		if (scans.isEmpty()) {
			throw new IllegalArgumentException("参数异常，hbase中不存在对应的表");
		}

		// 在linux集群运行的时候，采用下面的设置方式
		// TableMapReduceUtil.initTableMapperJob(scans,
		// NewInstallUsersMapper.class, StatsUserDimension.class, Text.class,
		// job);

		// windows本地执行的时候采用下面的方式，addDependencyJars设置为true
		TableMapReduceUtil.initTableMapperJob(scans, NewInstallUserMapper.class, StatsUserDimension.class, Text.class,
				job, true);
	}

	/**
	 * 创建一个根据列名过滤数据的filter对象
	 * 
	 * @param columns
	 * @return
	 */
	private Filter getColumnFilter(String[] columns) {
		byte[][] prefixs = new byte[columns.length][];
		for (int i = 0; i < columns.length; i++) {
			prefixs[i] = Bytes.toBytes(columns[i]);
		}
		return new MultipleColumnPrefixFilter(prefixs);
	}
}
