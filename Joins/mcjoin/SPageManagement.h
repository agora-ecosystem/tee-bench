/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once
struct SPageManagement
{
	uint32 page;			// Number of a 64-bit chunk	
	uint32 offset;			// Bit offset from 64-bit chunk
	
	SPageManagement() : page(0), offset(0)
	{
	}

	__forceinline void set(uint32 page, uint32 offset)
	{
		this->page = page;
		this->offset = offset;
	}


	// General-purpose offset calculator - the multiplication will make it a bit expensive.
	__forceinline void calculateOffset(uint32 itemCount, uint32 bitLength)
	{
		// Thanks to Phil Ward for the idea of performing a modulus operation with a logical AND
		uint64_t totalBits = itemCount * bitLength;
		
		//page = uint32( totalBits / 64 );
		page = uint32( totalBits >> 6);
				
		//offset = totalBits % 64;
		offset = totalBits & 63;	
	}
};