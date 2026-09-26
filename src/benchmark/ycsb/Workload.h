//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "Query.h"
#include "Context.h"
#include "../../common/Time.h"
#include <glog/logging.h>
#include <cstring>

namespace star
{
	namespace ycsb
	{
		std::shared_ptr<State> init_value()
		{
			std::shared_ptr<State> ptr = std::make_shared<State>();
			for (std::uint32_t i = 0; i < 10; i++)
			{
				ptr->dirty[i] = false;
			}
			memset(ptr->f0, '0', FIELD_SIZE);
			memset(ptr->f1, '1', FIELD_SIZE);
			memset(ptr->f2, '2', FIELD_SIZE);
			memset(ptr->f3, '3', FIELD_SIZE);
			memset(ptr->f4, '4', FIELD_SIZE);
			memset(ptr->f5, '5', FIELD_SIZE);
			memset(ptr->f6, '6', FIELD_SIZE);
			memset(ptr->f7, '7', FIELD_SIZE);
			memset(ptr->f8, '8', FIELD_SIZE);
			memset(ptr->f9, '9', FIELD_SIZE);
			ptr->f_ptr[0] = ptr->f0;
			ptr->f_ptr[1] = ptr->f1;
			ptr->f_ptr[2] = ptr->f2;
			ptr->f_ptr[3] = ptr->f3;
			ptr->f_ptr[4] = ptr->f4;
			ptr->f_ptr[5] = ptr->f5;
			ptr->f_ptr[6] = ptr->f6;
			ptr->f_ptr[7] = ptr->f7;
			ptr->f_ptr[8] = ptr->f8;
			ptr->f_ptr[9] = ptr->f9;
			return ptr;
		}

		std::shared_ptr<State> init_pointer()
		{
			return nullptr;
		}

		class Workload
		{
		public:
			using RandomType = Random;

			Workload(RandomType &random)
				: random(random)
			{
			}

			void generate_request()
			{
				auto random_seed = Time::now();
				random.set_seed(random_seed);
				int x = random.uniform_dist(1, 100);

				if (workloadType == YCSBWorkloadType::A)
				{
				}
				else if (workloadType == YCSBWorkloadType::B)
				{
				}
				else if (workloadType == YCSBWorkloadType::C)
				{
				}
				else if (workloadType == YCSBWorkloadType::D)
				{
				}
				else
				{
					LOG(ERROR) << "Invalid workload type: " << static_cast<int>(workloadType);
					exit(-1);
				}
			}

		private:
			RandomType &random;
			YCSBWorkloadType workloadType;
		};

	} // namespace ycsb
} // namespace star
