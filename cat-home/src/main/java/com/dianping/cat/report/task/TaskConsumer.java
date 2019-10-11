/*
 * Copyright (c) 2011-2018, Meituan Dianping. All Rights Reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dianping.cat.report.task;

import java.util.Calendar;

import com.dianping.cat.Cat;
import com.dianping.cat.configuration.NetworkInterfaceManager;
import com.dianping.cat.core.dal.Task;

public abstract class TaskConsumer implements org.unidal.helper.Threads.Task {

	public static final int STATUS_TODO = 1;

	public static final int STATUS_DOING = 2;

	public static final int STATUS_DONE = 3;

	public static final int STATUS_FAIL = 4;

	private static final int MAX_TODO_RETRY_TIMES = 1;

	private long m_nanos = 2L * 1000 * 1000 * 1000;

	private volatile boolean m_running = true;

	private volatile boolean m_stopped = false;

	/**
	 * 获取当前系统时间的分钟，大于等于10分为true，小于10分为false
	 * @return
	 */
	public boolean checkTime() {
		Calendar cal = Calendar.getInstance();
		int minute = cal.get(Calendar.MINUTE);

		if (minute >= 10) {
			return true;
		} else {
			return false;
		}
	}

	protected abstract Task findDoingTask(String consumerIp);

	protected abstract Task findTodoTask();

	protected String getLoaclIp() {
		return NetworkInterfaceManager.INSTANCE.getLocalHostAddress();
	}

	protected long getSleepTime() {
		return m_nanos;
	}

	public boolean isStopped() {
		return m_stopped;
	}

	protected abstract boolean processTask(Task doing);

	@Override
	public void run() {
		String localIp = getLoaclIp();
		while (m_running) {
			try {
				//当前时间大于等于10分时才会进入程序
				if (checkTime()) {
					Task task = findDoingTask(localIp);
					if (task == null) {
						task = findTodoTask();
					}
					boolean again = false;
					if (task != null) {
						try {
							task.setConsumer(localIp);
							//判断任务的状态是否为2，正在执行中
							if (task.getStatus() == TaskConsumer.STATUS_DOING || updateTodoToDoing(task)) {
								int retryTimes = 0;
								while (!processTask(task)) {
									retryTimes++;
									if (retryTimes < MAX_TODO_RETRY_TIMES) {
										taskRetryDuration();
									} else {
										//失败次数为1时，即修改任务状态为失败
										updateDoingToFailure(task);
										again = true;
										break;
									}
								}
								//again 为false 时，代表任务正常执行完毕
								if (!again) {
									//任务结束，修改状态
									updateDoingToDone(task);
								}
							}
						} catch (Throwable e) {
							Cat.logError(task.toString(), e);
						}
					} else {
						taskNotFoundDuration();
					}
				} else {
					try {
						//出错睡眠1分钟
						Thread.sleep(60 * 1000);
					} catch (InterruptedException e) {
						// Ignore
					}
				}
			} catch (Throwable e) {
				Cat.logError(e);
			}
		}
		m_stopped = true;
	}

	public void stop() {
		m_running = false;
	}

	protected abstract void taskNotFoundDuration();

	protected abstract void taskRetryDuration();

	protected abstract boolean updateDoingToDone(Task doing);

	protected abstract boolean updateDoingToFailure(Task todo);

	/**
	 * 修改任务 状态，改为正在进行
	 * @param todo
	 * @return
	 */
	protected abstract boolean updateTodoToDoing(Task todo);
}
