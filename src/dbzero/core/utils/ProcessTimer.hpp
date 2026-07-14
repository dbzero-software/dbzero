// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#pragma once

#include "ChronoMeter.hpp"
#include <dbzero/core/threading/ProgressiveMutex.hpp>

#include <map>
#include <string>
#include <iostream>

namespace db0

{

	/**
	 * Process timer is class to measure time spent on specific process
	 * and showing this time divided into sub-processes
	 */
	class ProcessTimer
	{
		mutable db0::progressive_mutex m_mutex;
	public :

		struct Task
		{
			std::string m_name;
			ChronoMeter m_meter;
			std::map<std::string, Task> m_sub_tasks;

			Task(const std::string &name, bool paused);

			void addSubTask(const Task &task);

			/**
             * @param parent_task_time number of seconds
             * @param crop_at tells to ignore tasks accounting for specific fraction of time (>5% by default)
             */
            std::ostream &printLog(std::ostream &os, double crop_at = 0.05, double parent_task_time = -1.0, int indent = 0, 
                double whole_time = -1.0) const;

			/**
             * Pause this task (and all sub-tasks)
             */
			void pause();

			/**
             * Merge with some other task
             */
			void mergeWith(const Task&);
		};

	private :
		Task m_task;
		ProcessTimer *m_parent_task = nullptr;
        ProcessTimer &m_root_task;

	public :
		/**
         * Create root timer (main task)
         */
		ProcessTimer(const std::string &task_name, bool paused = false);

		/**
         * Create task timer (sub-task)
         * when destroyed will report task time back to parent
         */
		ProcessTimer(const std::string &task_name, ProcessTimer &parent_task, bool paused = false);

		/**
         * @param task_name arbitrary task name
         * @param parent_task nullptr is acceptable
         */
		ProcessTimer(const std::string &task_name, ProcessTimer *parent_task, bool paused = false);
		~ProcessTimer();

		/**
         * Prints out collected information to output stream "os"
         * @param crop_at tells to ignore tasks accounting for specific fraction of time (>5% by default)
         */
		std::ostream &printLog(std::ostream &os, double crop_at = 0.05) const;
		double getTaskTime() const;

		const Task &getTask() const;

		/**
         * Pause this task (all all sub-tasks)
         */
		void pause();

		/**
         * Start / resume timer (only this task, not sub-tasks)
         */
		void start();

		/**
         * Get task duration in seconds
         */
		double getTaskDuration() const;
		void operator=(const ProcessTimer &);

		/**
		 * Get root task's timer (of self if this is the root task)
		 * @return valid pointer
		 */
		ProcessTimer *getRootTaskTimer ();

		/**
		 * Instruct root ProcessTimer to generate logs with specific logstream
		 */
		void showLogs(std::ostream &);

	protected :
		std::ostream *m_logs = nullptr;
		void onSubTaskStarted (ProcessTimer &sub_task);
		void onSubTaskFinished(ProcessTimer &sub_task);

		std::ostream &log();
	};

}
