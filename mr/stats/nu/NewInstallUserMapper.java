package com.transformer.mr.stats.nu;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.transformer.common.DateEnum;
import com.transformer.common.EventLogConstants;
import com.transformer.common.GlobalConstants;
import com.transformer.common.KpiType;
import com.transformer.dimension.key.base.BrowserDimension;
import com.transformer.dimension.key.base.DateDimension;
import com.transformer.dimension.key.base.KpiDimension;
import com.transformer.dimension.key.base.PlatformDimension;
import com.transformer.dimension.key.stats.StatsCommonDimension;
import com.transformer.dimension.key.stats.StatsUserDimension;
import com.transformer.util.TimeUtil;

/**
 * 处理HBase中的数据，然后封装成为key/value进行输出
 *
 */
public class NewInstallUserMapper extends TableMapper<StatsUserDimension, Text> {
	private static final Logger logger = Logger.getLogger(NewInstallUserMapper.class);
	private static final byte[] family = EventLogConstants.BYTES_EVENT_LOGS_FAMILY_NAME;
	private StatsUserDimension outputKey = new StatsUserDimension();
	private Text outputValue = new Text();
	private KpiDimension newInstallUserKpiDimension = new KpiDimension(KpiType.NEW_INSTALL_USER.name);
	private KpiDimension browserNewInstallUserKpiDimension = new KpiDimension(KpiType.BROWSER_NEW_INSTALL_USER.name);
	private BrowserDimension defaultBroserDimension = new BrowserDimension("", "");

	long startDay, endDay; // 天维度的时间范围:[)
	long startWeek, endWeek; // 周维度的时间范围:[)
	long startMonth, endMonth; // 月维度的时间范围:[)

	@Override
	protected void setup(Context context) throws IOException, InterruptedException {
		// 进行时间范围的初始化
		// 1. 获取上下文中的运行时间参数
		String date = context.getConfiguration().get(GlobalConstants.RUNNING_DATE_PARAMES);
		// 2. 将参数转换为long
		long dateTime = TimeUtil.parseString2Long(date);
		// 3. 构建时间范围
		this.startDay = dateTime;
		this.endDay = this.startDay + GlobalConstants.DAY_OF_MILLISECONDS;
		this.startWeek = TimeUtil.getFirstDayOfThisWeek(dateTime);
		this.endWeek = TimeUtil.getFirstDayOfNextWeek(dateTime);
		this.startMonth = TimeUtil.getFirstDayOfThisMonth(dateTime);
		this.endMonth = TimeUtil.getFirstDayOfNextMonth(dateTime);
	}

	@Override
	protected void map(ImmutableBytesWritable key, Result value, Context context)
			throws IOException, InterruptedException {
		// 1. 获取数据 => 从value中获取数据
		String platformName = this.getValue(value, EventLogConstants.LOG_COLUMN_NAME_PLATFORM);
		String platformVersion = this.getValue(value, EventLogConstants.LOG_COLUMN_NAME_VERSION);
		String serverTime = this.getValue(value, EventLogConstants.LOG_COLUMN_NAME_SERVER_TIME);
		String browserName = this.getValue(value, EventLogConstants.LOG_COLUMN_NAME_BROWSER_NAME);
		String browserVersion = this.getValue(value, EventLogConstants.LOG_COLUMN_NAME_BROWSER_VERSION);
		String uuid = this.getValue(value, EventLogConstants.LOG_COLUMN_NAME_UUID);

		// 2. 过滤数据
		if (StringUtils.isBlank(uuid) // uuid为空
				|| StringUtils.isBlank(platformName) // 平台名称为空
				|| StringUtils.isBlank(serverTime) // 服务器时间为空
				|| !StringUtils.isNumeric(serverTime) // 服务器时间不是数字的字符串
		) {
			logger.debug("数据异常");
			return; // 结束当前数据的处理
		}

		// 3. 封装数据 => 封装成为key/value键值对

		// 3.1 date维度：计算天、周、月三个时间维度的数据 ===>
		// 根据serverTime构建三个时间维度对象，表示的是serverTime所属的对应的时间维度
		long time = Long.valueOf(serverTime.trim());
		DateDimension dayOfDateDimension = DateDimension.buildDate(time, DateEnum.DAY);
		DateDimension weekOfDateDimension = DateDimension.buildDate(time, DateEnum.WEEK);
		DateDimension monthOfDateDimension = DateDimension.buildDate(time, DateEnum.MONTH);

		// 3.2 platform维度：(name,version), (name,all), (all,all)
		List<PlatformDimension> platforms = PlatformDimension.buildList(platformName, platformVersion);

		// 3.3 Browser维度: (name, version), (name, all)
		List<BrowserDimension> browsers = BrowserDimension.buildList(browserName, browserVersion);

		// 3.4 设置输出的uuid
		this.outputValue.set(uuid);

		// 4. 数据输出
		StatsCommonDimension statsCommonDimension = this.outputKey.getStatsCommon();
		for (PlatformDimension platform : platforms) {
			// 4.1 设置platform属性
			statsCommonDimension.setPlatform(platform);

			// 4.2 开始stats_user表对应的数据输出
			// 4.2.1 给定一个标志的KPI维度
			statsCommonDimension.setKpi(newInstallUserKpiDimension);
			// 4.2.2 需要给定一个占位的特殊browser位置值
			this.outputKey.setBrowser(this.defaultBroserDimension);
			// 4.2.3 设置date维度，并进行输出输出
			// 天维度
			if (time >= this.startDay && time < this.endDay) {
				statsCommonDimension.setDate(dayOfDateDimension);
				context.write(this.outputKey, this.outputValue);
			}

			// 周维度
			if (time >= this.startWeek && time < this.endWeek) {
				statsCommonDimension.setDate(weekOfDateDimension);
				context.write(this.outputKey, this.outputValue);
			}

			// 月维度
			if (time >= this.startMonth && time < this.endMonth) {
				statsCommonDimension.setDate(monthOfDateDimension);
				context.write(this.outputKey, this.outputValue);
			}

			// 4.3 开始stats_device_browser数据的输出
			// 4.3.1 给定一个标志的KPI维度
			statsCommonDimension.setKpi(browserNewInstallUserKpiDimension);
			for (BrowserDimension browser : browsers) {
				// 4.3.2 设置对应的browser
				this.outputKey.setBrowser(browser);

				// 4.3.3 设置date维度并进行输出
				// 天维度
				if (time >= this.startDay && time < this.endDay) {
					statsCommonDimension.setDate(dayOfDateDimension);
					context.write(this.outputKey, this.outputValue);
				}

				// 周维度
				if (time >= this.startWeek && time < this.endWeek) {
					statsCommonDimension.setDate(weekOfDateDimension);
					context.write(this.outputKey, this.outputValue);
				}

				// 月维度
				if (time >= this.startMonth && time < this.endMonth) {
					statsCommonDimension.setDate(monthOfDateDimension);
					context.write(this.outputKey, this.outputValue);
				}
			}
		}
	}

	/**
	 * 从value中获取对应column的值，如果不存在，返回null
	 * 
	 * @param value
	 * @param column
	 * @return
	 */
	private String getValue(Result value, String column) {
		return Bytes.toString(value.getValue(family, Bytes.toBytes(column)));
	}
}
