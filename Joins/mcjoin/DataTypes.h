/*
	This code is part of the MCJoin project.
	Authored by Steven Begley (sbegley@latrobe.edu.au) as part of my PhD candidature.
	La Trobe University,
	Melbourne, Australia.
*/
#pragma once

#ifdef _VGDB			// Need this to keep VisualGDB happy - define it somewhere if VisualGDB is being used (this must be first, or else _WIN32 will be picked up in VisualGDB)!
#include <stdint.h>
#define uint32 uint32_t
#define uint64 uint64_t
#define sint32 int32_t
#define sint64 int64_t
#define __forceinline __attribute__((always_inline))
#elif _WIN32
#define uint32 unsigned __int32
#define uint64 unsigned __int64
#define sint32 signed __int32
#define sint64 signed __int64
#else
#include <stdint.h>
#define uint32 uint32_t
//#define uint64 uint64_t
#define sint32 int32_t
#define sint64 int64_t
#define __forceinline __attribute__((always_inline))
#endif