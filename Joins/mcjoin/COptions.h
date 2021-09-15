/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once
#include "DataTypes.h"
#include "CRelation.h"
#include <iostream>
//#include <boost/filesystem.hpp>
#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "pcm_commons.h"
#include "Logger.h"
#else
#include "Enclave.h"
#include "Enclave_t.h"
#include <libcxx/string>
#endif
class COptions
{
public:

	COptions() : _threads(1), _bitRadixLength(24), _maxBitsPerFlipFlopPass(8), _flipFlopCardinality(16000000), _packedPartitionMemoryCardinality(16000000), _outputBufferCardinality(1000), _relationR(NULL), _relationS(NULL), _memoryConstraintMB(0), _loadBinaryRelations(false)
	{		
	}

	~COptions()
	{
		if(_relationR != NULL)
		{
			delete _relationR;
		}
		if(_relationS != NULL)
		{
			delete _relationS;
		}
	}

	bool getLoadBinaryRelations()
	{
		return _loadBinaryRelations;
	}

	void setLoadBinaryRelations(bool value)
	{
		_loadBinaryRelations = value;
	}

	uint32 getThreads()
	{
		return _threads;
	}

	void setThreads(uint32 value)
	{
		_threads = value;
	}

	uint32 getBitRadixLength()
	{
		return _bitRadixLength;
	}

	void setBitRadixLength(uint32 value)
	{
		_bitRadixLength = value;
	}
	
	uint32 getMaxBitsPerFlipFlopPass()
	{
		return _maxBitsPerFlipFlopPass;
	}

	void setMaxBitsPerFlipFlopPass(uint32 value)
	{
		_maxBitsPerFlipFlopPass = value;
	}

	uint32 getFlipFlopCardinality()
	{
		return _flipFlopCardinality;
	}

	void setFlipFlopCardinality(uint32 value)
	{
		_flipFlopCardinality = value;
	}

	uint32 getPackedPartitionMemoryCardinality()
	{
		return _packedPartitionMemoryCardinality;
	}

	void setPackedPartitionMemoryCardinality(uint32 value)
	{
		_packedPartitionMemoryCardinality = value;
	}

	uint32 getOutputBufferCardinality()
	{
		return _outputBufferCardinality;
	}

	void setOutputBufferCardinality(uint32 value)
	{
		_outputBufferCardinality = value;
	}

	relation_t* getRelationR()
	{
		return _relationR;
	}

	void setRelationR(relation_t *value)
	{
		_relationR = value;
	}

	string getRelationRFilename()
	{
		return _relationRFilename;
	}

	void setRelationRFilename(string value)
	{
		_relationRFilename = value;
	}

    relation_t* getRelationS()
	{
		return _relationS;
	}	

	void setRelationS(relation_t *value)
	{
		_relationS = value;
	}

	string getRelationSFilename()
	{
		return _relationSFilename;
	}

	void setRelationSFilename(string value)
	{
		_relationSFilename = value;
	}


	uint32 getMemoryConstraintMB()
	{
		return _memoryConstraintMB;
	}

	void setMemoryConstraintMB(uint32 value)
	{
		_memoryConstraintMB = value;
	}

	bool load(const string &configFile)
	{
		bool result(true);
//		ifstream config(configFile);
//
//		if(config.good())
//		{
//			config >> _threads >> _bitRadixLength >> _maxBitsPerFlipFlopPass >> _memoryConstraintMB >> _flipFlopCardinality >> _packedPartitionMemoryCardinality >> _outputBufferCardinality;
//			config >> boolalpha >> _loadBinaryRelations >> _relationRFilename >> _relationSFilename;
//
//			if(_loadBinaryRelations)
//			{
//				_relationRFilename += ".dat";
//				_relationSFilename += ".dat";
//			}
//			else
//			{
//				_relationRFilename += ".txt";
//				_relationSFilename += ".txt";
//			}
//
//			if(file_exists(_relationRFilename))
//			{
//				_relationR = new CRelation();
//				_relationR->load(_relationRFilename.c_str(), _loadBinaryRelations);
//				//options->getRelationR()->showDebug("R");
//			}
//			else
//			{
//				cout << "File not found for relation R [" << _relationRFilename << "]" << endl;
//				result = false;
//			}
//
//			if(file_exists(_relationSFilename))
//			{
//				_relationS = new CRelation();
//				_relationS->load(_relationSFilename.c_str(), _loadBinaryRelations);
//				//options->getRelationR()->showDebug("S");
//			}
//			else
//			{
//				cout << "File not found for relation S [" << _relationSFilename << "]" << endl;
//				result = false;
//			}
//		}
//		else
//		{
//			result = false;
//		}
//
//		config.close();
//
		return result;
	}

	void save(const string &configFile)
	{
//		ofstream config(configFile);
//
//		config << _threads << endl;
//		config <<_bitRadixLength << endl;
//		config << _maxBitsPerFlipFlopPass << endl;
//		config <<_memoryConstraintMB << endl;
//		config << _flipFlopCardinality << endl;
//		config <<_packedPartitionMemoryCardinality << endl;
//		config <<_outputBufferCardinality << endl;
//		config << boolalpha << _loadBinaryRelations << endl;
//		config <<_relationRFilename << endl;
//		config <<_relationSFilename << endl;
//
//		config.close();
	}

//	void getCondensedOptions(stringstream* output)
//	{
//		*output << _threads << "," << _bitRadixLength << "," << _maxBitsPerFlipFlopPass << "," << _memoryConstraintMB << "," << _packedPartitionMemoryCardinality << "," << _flipFlopCardinality << "," << _outputBufferCardinality << "," << boost::filesystem::path(_relationRFilename).stem().string() << "," << boost::filesystem::path(_relationSFilename).stem().string();
//	}

private:
	inline bool file_exists(const string &name) 
	{
//		ifstream f(name.c_str());
//
//		if (f.good()) {
//			f.close();
//			return true;
//		} else {
//			f.close();
//			return false;
//		}
        return true;
	}

	uint32 _threads;
	uint32 _bitRadixLength;
	uint32 _maxBitsPerFlipFlopPass;
	uint32 _flipFlopCardinality;
	uint32 _packedPartitionMemoryCardinality;
	uint32 _outputBufferCardinality;
	uint32 _memoryConstraintMB;	// In megabytes
	string _relationRFilename;
	string _relationSFilename;
	relation_t *_relationR;
	relation_t *_relationS;
	bool _loadBinaryRelations;
};