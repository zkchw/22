package com.transformer.mr;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * @param <K> Reducer输出的Key类型
 * @param <V> Reducer输出的Value类型
 */
public class DemoOutputFormat<K,V> extends OutputFormat<K, V> {

	@Override
	public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/**
		 * 根据上下文获取一个具体写出数据的对象
		 * reduce的数据写出由RecordWriter对象来操作
		 * reduce方法中调用context.write输出数据后，直接进入RecordWriter类中的相关方法(write)
		 */
		return null;
	}

	@Override
	public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/**
		 * 在job运行前进行job运行前置条件的检测，比如：文件夹是否存在、表是否存在
		 * 如果检测不通过，直接抛出异常
		 */
	}

	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/**
		 * 获取的是一个输出的提交类对象，一般不需要自己实现，可以使用Hadoop中默认的FileOutputCommitter
		 */
		return null;
	}
	
	/**
	 * 具体的数据输出类
	 * @param <K>
	 * @param <V>
	 */
	public static class DemoRecordWriter<K,V> extends RecordWriter<K,V> {

		@Override
		public void write(K key, V value) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/**
			 * 写出key/value键值对
			 * reducer方法中输出后，最终是调用该方法进行具体的数据输出的
			 */
			
		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/**
			 * 关闭资源
			 */
		}
		
	}

}
