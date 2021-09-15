/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once

#include "COptions.h"
#include "CPackedPartitionMemory.h"
#ifdef NATIVE_COMPILATION
#include "native_ocalls.h"
#include "Logger.h"
#else
#include <libcxx/cmath>
#include "Enclave.h"
#include "Enclave_t.h"
#endif

class CMemoryManagement
{
public:
	CMemoryManagement() : _r_bias(0)
	{

	}

    bool optimiseForMemoryConstraint(uint32_t R_cardinality,
                                     uint64_t total_memoryAllocated,
                                     uint32_t radixBits)
    {
        bool result(true);

        uint32 histogramSize;

        uint64_t R_maxTuples;

        uint32 R_idealTuples;
        uint32 S_idealTuples;

        uint64_t R_chunkMemoryAllocated;
        uint64_t S_chunkMemoryAllocated;

        double R_chunkCount;

        total_memoryAllocated = total_memoryAllocated * 1024 * 1024;	// Measured in bytes

        // Remove histogram cost from memory allocation
        histogramSize = 4 * uint32(std::pow(2.0, (double)radixBits));

        if(histogramSize >= total_memoryAllocated)
        {
            return false;	// Not enough memory to perform the join with the supplied parameters
        }

        //text.Format(_T("Histogram / TotalMem ratio: %.2f"), ((float)histogramSize / (float)total_memoryAllocated));
        total_memoryAllocated -= histogramSize;

        // Work out a conservative estimation of the max number of compressed tuples we can force into an R chunk with the available memory
        R_maxTuples = numberOfPackedRecords((total_memoryAllocated), 32 - radixBits);

        // How many chunks do we need to process all of R? (whole chunks only!)
        R_chunkCount = ceil((float)R_cardinality / (float)R_maxTuples);


        // Now we work out the minimum allocation needed to create this chunk size. How many tuples are in the chunk?
        R_idealTuples = (uint32)ceil(R_cardinality / R_chunkCount);

        CPackedPartitionMemory ppm(radixBits);
        ppm.resetBuffers(R_idealTuples);
        R_chunkMemoryAllocated = ppm.getTotalMemoryAllocated();

        S_chunkMemoryAllocated = total_memoryAllocated - R_chunkMemoryAllocated;
        S_idealTuples = (uint32)(S_chunkMemoryAllocated / 16);


        if(((double)S_idealTuples / (double)R_idealTuples) < (1.0f/20.0f))
        {
            ++R_chunkCount;
            R_idealTuples = (uint32)ceil(R_cardinality / R_chunkCount);

            ppm.resetBuffers(R_idealTuples);
            R_chunkMemoryAllocated = ppm.getTotalMemoryAllocated();

            S_chunkMemoryAllocated = total_memoryAllocated - R_chunkMemoryAllocated;
            S_idealTuples = (uint32)(S_chunkMemoryAllocated / 16);
        }


        _r_idealTuples = R_idealTuples;
        _s_idealTuples = S_idealTuples;
        _r_bias = (float)R_chunkMemoryAllocated / (float)total_memoryAllocated;

        return result;
    }


    bool optimiseForMemoryConstraint(COptions *options)
	{
		bool result(true);

		uint64_t total_memoryAllocated;
		uint32 radixBits;
		uint32 histogramSize;
		uint32 R_cardinality;

		uint64_t R_maxTuples;

		uint32 R_idealTuples;
		uint32 S_idealTuples;

		uint64_t R_chunkMemoryAllocated;
		uint64_t S_chunkMemoryAllocated;

		double R_chunkCount;

		R_cardinality = options->getRelationR()->num_tuples;
		total_memoryAllocated = options->getMemoryConstraintMB() * 1024 * 1024;	// Measured in bytes

		radixBits = options->getBitRadixLength();

		// Remove histogram cost from memory allocation
		histogramSize = 4 * uint32(std::pow(2.0, (double)radixBits));

		if(histogramSize >= total_memoryAllocated)
		{
			return false;	// Not enough memory to perform the join with the supplied parameters
		}

		//text.Format(_T("Histogram / TotalMem ratio: %.2f"), ((float)histogramSize / (float)total_memoryAllocated));
		total_memoryAllocated -= histogramSize;

		// Work out a conservative estimation of the max number of compressed tuples we can force into an R chunk with the available memory
		R_maxTuples = numberOfPackedRecords((total_memoryAllocated), 32 - radixBits);

		// How many chunks do we need to process all of R? (whole chunks only!)
		R_chunkCount = ceil((float)R_cardinality / (float)R_maxTuples);


		// Now we work out the minimum allocation needed to create this chunk size. How many tuples are in the chunk?		
		R_idealTuples = (uint32)ceil(R_cardinality / R_chunkCount);

		CPackedPartitionMemory ppm(radixBits);
		ppm.resetBuffers(R_idealTuples);
		R_chunkMemoryAllocated = ppm.getTotalMemoryAllocated();

		S_chunkMemoryAllocated = total_memoryAllocated - R_chunkMemoryAllocated;
		S_idealTuples = (uint32)(S_chunkMemoryAllocated / 16);


		if(((double)S_idealTuples / (double)R_idealTuples) < (1.0f/20.0f))
		{
			++R_chunkCount;
			R_idealTuples = (uint32)ceil(R_cardinality / R_chunkCount);		

			ppm.resetBuffers(R_idealTuples);
			R_chunkMemoryAllocated = ppm.getTotalMemoryAllocated();

			S_chunkMemoryAllocated = total_memoryAllocated - R_chunkMemoryAllocated;
			S_idealTuples = (uint32)(S_chunkMemoryAllocated / 16);
		}


		_r_idealTuples = R_idealTuples;
		_s_idealTuples = S_idealTuples;
		_r_bias = (float)R_chunkMemoryAllocated / (float)total_memoryAllocated;	

		return result;
	}

	double getRBias()
	{
		return _r_bias;
	}

	uint32 getRIdealTuples()
	{
		return _r_idealTuples;
	}

	uint32 getSIdealTuples()
	{
		return _s_idealTuples;
	}


private:
	double _r_bias;
	uint32 _r_idealTuples;
	uint32 _s_idealTuples;

	// The LambertW function was authored by Keith Briggs.
	// http://keithbriggs.info/software.html
	double LambertW(const double z) 
	{
		int i; 
		const double eps=4.0e-16, em1=0.3678794411714423215955237701614608; 
		double p,e,t,w;


		if (0.0==z) return 0.0;

		if (z<-em1+1e-4) { // series near -em1 in sqrt(q)
			double q=z+em1,r=sqrt(q),q2=q*q,q3=q2*q;
			return 
				-1.0
				+2.331643981597124203363536062168*r
				-1.812187885639363490240191647568*q
				+1.936631114492359755363277457668*r*q
				-2.353551201881614516821543561516*q2
				+3.066858901050631912893148922704*r*q2
				-4.175335600258177138854984177460*q3
				+5.858023729874774148815053846119*r*q3
				-8.401032217523977370984161688514*q3*q;  // error approx 1e-16
		}
		/* initial approx for iteration... */

		if (z<1.0) 
		{ /* series near 0 */
			p=sqrt(2.0*(2.7182818284590452353602874713526625*z+1.0));
			w=-1.0+p*(1.0+p*(-0.333333333333333333333+p*0.152777777777777777777777)); 
		} 
		else 
		{
			w=log(z); /* asymptotic */
		}
		if (z>3.0) w-=log(w); /* useful? */

		for (i=0; i<10; i++) { /* Halley iteration */
			e=exp(w); 
			t=w*e-z;
			p=w+1.0;
			t/=e*p-0.5*(p+1.0)*t/p; 
			w-=t;
			if (fabs(t)<eps*(1.0+fabs(w))) return w; /* rel-abs error */
		}

		ocall_exit(1);
	}

	uint32 numberOfPackedRecords(uint64_t memoryLimit, uint32 bitsToStore)
	{
		double result = 0.5 * exp(LambertW(16.0 * log(2.0) * (memoryLimit - 16.0) * exp(bitsToStore * log(2.0))) - (bitsToStore * log(2.0)));

		return (uint32)result;
	}

};