//
// Created by Yibo Huang on 8/8/24.
//

#pragma once

#include <atomic>

#include "Context.h"
#include "Cacheline.h"

#ifndef USE_RDMA
#include "cxlalloc.h"
#else
#include "rdma/RdmaRegion.h"
#endif
#include <glog/logging.h>

namespace star
{

	class CXLMemory
	{
	public:
		// statistics
		enum
		{
			TOTAL_USAGE,
			TOTAL_HW_CC_USAGE,
			INDEX_USAGE,
			METADATA_USAGE,
			DATA_USAGE,
			TRANSPORT_USAGE,
			MISC_USAGE,
			INDEX_ALLOCATION,
			METADATA_ALLOCATION,
			DATA_ALLOCATION,
			TRANSPORT_ALLOCATION,
			MISC_ALLOCATION,
			INDEX_FREE,
			METADATA_FREE,
			DATA_FREE,
			TRANSPORT_FREE,
			MISC_FREE
		};

		// Account for allocations before forwarding to the selected backend.
		void *cxlalloc_malloc_wrapper(uint64_t size, int category)
		{
			// collect statistics
			switch (category)
			{
			case INDEX_ALLOCATION:
				size_total_hw_cc_usage.fetch_add(size);
				size_index_usage.fetch_add(size);
				break;
			case METADATA_ALLOCATION:
				if (context.migration_policy == "LRU")
				{
					size_total_hw_cc_usage.fetch_add(size + 24);
				}
				else
				{
					size_total_hw_cc_usage.fetch_add(size);
				}
				size_total_hw_cc_usage.fetch_add(size);
				size_metadata_usage.fetch_add(size);
				break;
			case DATA_ALLOCATION:
				if (context.enable_scc == false)
				{
					size_total_hw_cc_usage.fetch_add(size);
				}
				size_data_usage.fetch_add(size);
				break;
			case TRANSPORT_ALLOCATION:
				size_transport_usage.fetch_add(size);
				break;
			case MISC_ALLOCATION:
				size_total_hw_cc_usage.fetch_add(size);
				size_misc_usage.fetch_add(size);
				break;
			default:
				CHECK(0);
			}

#ifndef USE_RDMA
			// Index buckets and nodes contain independently written cache lines.
			return cxlalloc_memalign(size, CACHELINE_SIZE);
#else
			return reinterpret_cast<void*>(g_rdma.alloc(size));
#endif
		}

	private:
		Context context;

		std::atomic<uint64_t> size_index_usage{0};
		std::atomic<uint64_t> size_metadata_usage{0};
		std::atomic<uint64_t> size_data_usage{0};
		std::atomic<uint64_t> size_transport_usage{0};
		std::atomic<uint64_t> size_misc_usage{0};

		std::atomic<uint64_t> size_total_hw_cc_usage{0};
	};

	extern CXLMemory cxl_memory;

}
