package com.transformer.mr;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @param <K> Mapper类的输入key
 * @param <V> Mapper类的输入value
 */
public class DemoInputFormat<K, V> extends InputFormat<K, V>{

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/**
		 * 根据job设置的参数，构建输入数据的分片，在job提交运行前执行该动作(在client端执行)；
		 * MapReduce框架会将该方法返回的list集合上传到HDFS上，供MapTask执行
		 * */
		return null;
	}

	@Override
	public RecordReader<K, V> createRecordReader(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		/**
		 * MapTask根据对应的split对象(AM传递给MapTask)回去该分片对应的数据读取器
		 */
		return null;
	}
	
	/**
	 * 具体对应的数据读取器
	 * @param <K>
	 * @param <V>
	 */
	public static class DemoRecordReader<K,V> extends RecordReader<K,V> {

		@Override
		public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/**
			 * 初始化操作：
			 *  将split强制转换为具体的split对象
			 *  基于split构建数据输入流
			 * */
			
		}

		@Override
		public boolean nextKeyValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/**
			 * 是否有下一个key/value值，如果有，返回true；否则返回false
			 */
			return false;
		}

		@Override
		public K getCurrentKey() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/**
			 * 获取当前的key
			 */
			return null;
		}

		@Override
		public V getCurrentValue() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/**
			 * 获取当前value
			 */
			return null;
		}

		
		public float getProgress() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			/**
			 * 该MapTask读取split数据的进度大小，可以一直返回0，取值范围是:[0,1]
			 */
			return 0;
		}

	
		public void close() throws IOException {
			// TODO Auto-generated method stub
			/**
			 * 关闭资源
			 */
		}
		
	}
	
	/**
	 * 具体的分片实例对象
	 *
	 */
	public static class DemoInputSplit extends InputSplit implements Writable{
		// 一般情况下，会有一个开始偏移量和结束偏移量

		@Override
		public long getLength() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 获取长度
			return 0;
		}

		@Override
		public String[] getLocations() throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			// 控制数据执行本地化的返回值，返回数据分片所在的地址(host)数组
			// 可以返回一个空的数组，表示不启动数据本地化操作:return new String[0]; 
//			return null;
			return new String[0];
		}

		@Override
		public void write(DataOutput out) throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void readFields(DataInput in) throws IOException {
			// TODO Auto-generated method stub
			
		}
		
	}

}
