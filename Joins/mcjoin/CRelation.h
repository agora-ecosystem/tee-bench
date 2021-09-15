/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once

#include <iostream>
#include <fstream>
#include <stdlib.h>
#include "util.h"
//#include <boost/random.hpp>
//#include <boost/filesystem.hpp>
#include "DataTypes.h"
#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#endif

using namespace std;

class CRelation
{
public:
	CRelation() : _bat(NULL), _cardinality(0)
	{
	}

	CRelation(uint32 cardinality) : _bat(NULL), _cardinality(cardinality)
	{
		if(cardinality > 0)
		{
			_bat = (tuple_t*)malloc(cardinality * sizeof(tuple_t));
			malloc_check(_bat);
		}
	}

	~CRelation(void)
	{
		if(_bat != nullptr)
		{
			free(_bat);
		}
	}

	__forceinline tuple_t* getBATPointer() const
	{
		return _bat;
	}

	__forceinline uint32 getCardinality() const
	{

		return _cardinality;
	}

	void showDebug(char *relation)
	{				
		logger(DBG, "Relation %s debug:\n", relation);

		tuple_t* p = _bat;
		for(uint32 index = 0; index < _cardinality; ++index)
		{						
			logger(DBG, "Key: %u Payload: %u\n", p->key, p->payload);
			++p;
		}
	}
	
private:

	tuple_t* _bat;
	uint32 _cardinality;
};