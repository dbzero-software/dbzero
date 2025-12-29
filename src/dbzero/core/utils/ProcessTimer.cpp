// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2025 DBZero Software sp. z o.o.

#include "ProcessTimer.hpp"
#include <dbzero/core/exception/Exceptions.hpp>
#include <dbzero/core/utils/null_stream.hpp>

namespace db0

{

	ProcessTimer::ProcessTimer(const std::string &task_name, bool paused)
		: ProcessTimer(task_name, nullptr, paused) 
    {
	}

	ProcessTimer::ProcessTimer(const std::string &task_name, ProcessTimer &parent_task, bool paused)
		: ProcessTimer(task_name, &parent_task, paused) 
    {
	}

	ProcessTimer::ProcessTimer(const std::string &task_name, ProcessTimer *parent_task, bool paused)
		: m_task(task_name, paused)
        , m_parent_task(parent_task)
        , m_root_task(*getRootTaskTimer())
    {
        if (m_parent_task) {
            m_parent_task->onSubTaskStarted(*this);
        }
	}

    ProcessTimer::Task::Task(const std::string &name, bool paused)
		: m_name(name)
        , m_meter(paused) 
    {
	}

	std::ostream &ProcessTimer::printLog(std::ostream &os, double crop_at) const 
    {
		db0::progressive_mutex::scoped_read_lock lock(m_mutex);
		return m_task.printLog(os, crop_at);
	}

	std::ostream &ProcessTimer::Task::printLog(std::ostream &os, double crop_at, double parent_task_time, 
        int indent, double whole_time) const 
    {
		double rate = 1.0;
		double s = m_meter.getSeconds();
		if (parent_task_time > 0) {
			rate = s / parent_task_time;
		}
		if (whole_time < 0) {
			whole_time = s;
		}
		// ignore tasks taking less than "crop_at" fraction of time
		if (!(rate < crop_at)) {
			for (int i = 0; i < indent; ++i) {
				os << "-";
			}
			double total = s / whole_time;

			os << m_name << " took " << m_meter.getSeconds() << " sec. (" << rate * 100.0 << "%";

			if (std::abs(rate - total) > 0.0001) {
				os << ", " << s / whole_time * 100.0 << "% of total";
			}

			os << ")" << std::endl;

			// then print all sub-tasks
			for (const auto &p: m_sub_tasks) {
				p.second.printLog(os, crop_at, s, indent + 2, whole_time);
			}
		}
		return os;
	}

	double ProcessTimer::getTaskTime() const {
		db0::progressive_mutex::scoped_read_lock lock(m_mutex);
		return m_task.m_meter.getSeconds();
	}

	ProcessTimer::~ProcessTimer() 
	{
		if (m_parent_task) {
			(*m_parent_task).onSubTaskFinished(*this);
		}
	}

	const ProcessTimer::Task &db0::ProcessTimer::getTask() const {
		return m_task;
	}

	void ProcessTimer::Task::mergeWith(const Task &other) 
    {
		m_meter += other.m_meter;
		for (const auto &p: other.m_sub_tasks) {
			auto it = m_sub_tasks.find(p.first);
			if (it == m_sub_tasks.end()) {
				m_sub_tasks.insert(p);
			} else {
				it->second.mergeWith(p.second);
			}
		}
	}

	void ProcessTimer::Task::addSubTask(const Task &task) 
    {
		auto it = m_sub_tasks.find(task.m_name);
		if (it != m_sub_tasks.end()) {
			// merge subtask trees
			it->second.mergeWith(task);
		} else {
			m_sub_tasks.emplace(task.m_name, task);
		}
	}

	void ProcessTimer::start() {
		m_task.m_meter.start();
	}

	double ProcessTimer::getTaskDuration() const {
		return m_task.m_meter.getSeconds();
	}

	void ProcessTimer::onSubTaskFinished (ProcessTimer &sub_task) 
    {
		db0::progressive_mutex::scoped_unique_lock lock(m_mutex);
		sub_task.pause();
		m_task.addSubTask(sub_task.m_task);
        // only when logs enabled, otherwise sent to null stream
//        m_root_task.log() << "Finished: " << sub_task.getTask().m_name << std::endl;
	}

    void db0::ProcessTimer::onSubTaskStarted (ProcessTimer &/*sub_task*/) {
        // only when logs enabled, otherwise sent to null stream
//        m_root_task.log() << "Started: " << sub_task.getTask().m_name << std::endl;
    }

	void ProcessTimer::Task::pause() 
    {
		m_meter.pause();
		for (auto &p: m_sub_tasks) {
			p.second.pause();
		}
	}

	void ProcessTimer::pause() {
		m_task.pause();
	}

	void ProcessTimer::operator=(const ProcessTimer &other) 
    {
		db0::progressive_mutex::scoped_unique_lock lock(m_mutex);
		db0::progressive_mutex::scoped_read_lock read_lock(other.m_mutex);
		m_task = other.m_task;
		m_parent_task = other.m_parent_task;
	}

	ProcessTimer *ProcessTimer::getRootTaskTimer() 
	{
		if (m_parent_task) {
			return m_parent_task->getRootTaskTimer();
		}
		return this;
	}
	
    void ProcessTimer::showLogs(std::ostream &logs)
	{
        if (m_parent_task) {
            THROWF (db0::InternalException) << "Logs can be collected only by the root task";
        }
        this->m_logs = &logs;
    }
	
    std::ostream &ProcessTimer::log()
	{
        if (m_logs) {
            return *m_logs;
        } else {
            return utils::nullStream;
        }
    }

}