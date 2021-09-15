/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once

#include "util.h"
#include "CFlipFlop.h"
#include "DataTypes.h"

class CFlipFlopBuffer
{
public:
	CFlipFlopBuffer(const relation_t* input, uint32 radixLength, uint32 maxDLength) : _input(input), _radixLength(radixLength), _maxDLength(maxDLength), _source(NULL), _destination(NULL)
	{		
	}

	~CFlipFlopBuffer()
	{
		if(_canDeleteSource)
		{
//			delete _source;
            free(_source->tuples);
		}
	}

    relation_t* doPartitioning(uint32 startIndex, uint32 endIndex, uint32 baseIndex, bool retainPayload)
	{
		uint32 D(0);
		uint32 bitsProcessed(0);
		CFlipFlop ff;
		const relation_t* tmp(NULL);
		uint32 itemCount(endIndex - startIndex);

		_source = _input;		
		_canDeleteSource = false;

		while(bitsProcessed != _radixLength)
		{
			D = _maxDLength;
			if( (D + bitsProcessed) > _radixLength)
			{
				D = _radixLength - bitsProcessed;
			}

			//
			if(_destination == NULL)
			{
//				_destination = new CRelation(itemCount);
                _destination = (relation_t*) malloc(sizeof(relation_t));
                malloc_check(_destination);
				_destination->tuples = (row_t *) malloc(itemCount * sizeof(row_t));
				malloc_check(_destination->tuples);
				_destination->num_tuples = itemCount;
			}			

			ff.setRelations(_source, _destination, itemCount);

			if(bitsProcessed == 0)
			{
				ff.go(D, bitsProcessed, startIndex, endIndex, baseIndex, retainPayload);
			}
			else
			{
				ff.go(D, bitsProcessed);
			}
			//

			bitsProcessed += D;

			if(bitsProcessed != _radixLength)
			{
				// Switch the pointers
				tmp = _source;
				_source = _destination;
				_destination = const_cast<relation_t *>(tmp);

				if(_canDeleteSource == false)
				{
//					_destination = new CRelation(itemCount);
                    _destination = (relation_t*) malloc(sizeof(relation_t));
                    malloc_check(_destination);
					_destination->tuples = (tuple_t*) malloc(itemCount * sizeof(tuple_t));
                    malloc_check(_destination->tuples);
                    _destination->num_tuples = itemCount;
					_canDeleteSource = true;
				}				
			}
		}

		return _destination;
	}

private:
	const relation_t* _input;

	const relation_t* _source;
    relation_t* _destination;

	uint32 _radixLength;
	uint32 _maxDLength;

	bool _canDeleteSource;
};