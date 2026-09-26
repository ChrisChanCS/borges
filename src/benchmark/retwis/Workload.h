//
// Created by Yi Lu on 7/25/18.
//

#pragma once

#include "../../common/Time.h"
#include "Query.h"
#include <glog/logging.h>
#include <cstring>

namespace star
{
	namespace retwis
	{
		std::shared_ptr<State> init_user(std::uint32_t username)
		{
			std::shared_ptr<State> ptr = std::make_shared<State>();
			ptr->user = new User();
			ptr->user->username = username;
			ptr->user->password = username;
			std::map<std::uint32_t, std::uint32_t> followers;
			star::Random random;
			auto random_seed = star::Time::now() ^ getpid() ^ (pthread_self() & 0xFFFFFFFF);
			random.set_seed(random_seed);
			while (followers.size() < FOLLOWERS_CNT)
			{
				std::uint32_t follower = random.next_uint32() % USER_CNT;
				if (followers.find(follower) == followers.end())
				{
					followers[follower] = 1;
					ptr->user->followers[followers.size() - 1] = follower;
				}
			}
			return ptr;
		}

		std::shared_ptr<State> init_post(std::uint32_t post_id)
		{
			std::shared_ptr<State> ptr = std::make_shared<State>();
			ptr->post = new Post();
			ptr->post->post_id = post_id;
			ptr->post->content = new char[CONTENT_SIZE];
			std::memset(ptr->post->content, '1', CONTENT_SIZE);
			return ptr;
		}

		std::shared_ptr<State> init_post()
		{
			std::shared_ptr<State> ptr = std::make_shared<State>();
			ptr->post = new Post();
			return ptr;
		}

		std::shared_ptr<State> init_timeline(std::uint32_t username, std::map<std::uint32_t, std::uint32_t> &initial_posts)
		{
			std::shared_ptr<State> ptr = std::make_shared<State>();
			ptr->timeline = new Timeline();
			ptr->timeline->username = username;
			for (auto &[post_id, timestamp] : initial_posts)
			{
				// Pack the post ID and timestamp into one uint64_t: Upper 32 bits: post ID; lower 32 bits: timestamp.
				std::uint64_t packed_value = (static_cast<std::uint64_t>(post_id) << 32) | timestamp;
				ptr->timeline->posts.emplace_back(packed_value);
				// index++;
			}
			return ptr;
		}
	} // namespace retwis
} // namespace star
