/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once

#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

class Timer
{
public:
	Timer() : _elapsedTime(0)
	{
//		_currentTime = std::chrono::high_resolution_clock::now();
		ocall_get_system_micros(&_currentTime);
		_previousTime = _currentTime;
	}

	~Timer()
	{
	}

	void update()
	{
//		_currentTime = std::chrono::high_resolution_clock::now();
//		_elapsedTime = std::chrono::duration_cast<std::chrono::microseconds>(_currentTime - _previousTime);
        ocall_get_system_micros(&_currentTime);
        _elapsedTime = _currentTime - _previousTime;
		_previousTime = _currentTime;
	}

	double getElapsedTime()
	{
		return _elapsedTime / 1000000.0;
	}

	uint64_t getElapsedTimeMicros()
    {
	    return _elapsedTime;
    }

private:
//	std::chrono::high_resolution_clock::time_point _currentTime;
//	std::chrono::high_resolution_clock::time_point _previousTime;
//	std::chrono::microseconds _elapsedTime;

	uint64_t _currentTime;
	uint64_t _previousTime;
	uint64_t _elapsedTime;
};