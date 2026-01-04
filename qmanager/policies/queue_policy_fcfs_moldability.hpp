/*****************************************************************************\
 * Copyright 2019 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef QUEUE_POLICY_FCFS_MOLDABILITY_HPP
#define QUEUE_POLICY_FCFS_MOLDABILITY_HPP

#include <jansson.h>
#include "qmanager/policies/base/queue_policy_base.hpp"

namespace Flux {
namespace queue_manager {
namespace detail {

template<class reapi_type>
class queue_policy_fcfs_moldability_t : public queue_policy_base_t {
   public:
    int run_sched_loop (void *h, bool use_alloced_queue) override;
    int reconstruct_resource (void *h, std::shared_ptr<job_t> job, std::string &R_out) override;
    int apply_params () override;
    const std::string_view policy () const override
    {
        return "fcfs_moldability";
    }
    int handle_match_success (flux_jobid_t jobid,
                              const char *status,
                              const char *R,
                              int64_t at,
                              double ov) override;
    int handle_match_failure (flux_jobid_t jobid, int errcode) override;
    int cancel (void *h,
                flux_jobid_t id,
                const char *R,
                bool noent_ok,
                bool &full_removal) override;
    int cancel (void *h, flux_jobid_t id, bool noent_ok) override;

   private:
    class selector_t {
        public:
         virtual ~selector_t() = default;
         virtual int select(queue_policy_fcfs_moldability_t *policy,
                            void *h,
                            json_t *task_counts,
                            json_t *durations) = 0;
        protected:
         std::pair<int,int> get_cores(void *h);
    };
    class selector_largest_fit_t final : public selector_t {
        public:
         int select(queue_policy_fcfs_moldability_t *policy,
                    void *h,
                    json_t *task_counts,
                    json_t *durations) override;
    };
    class selector_tanh_t final : public selector_t {
        public:
         int select(queue_policy_fcfs_moldability_t *policy,
                    void *h,
                    json_t *task_counts,
                    json_t *durations) override;
    };
    std::unique_ptr<selector_t> m_selector;
    std::string m_selector_name = "largest_fit";
    int select_from(void *h, json_t *task_counts, json_t *durations);
    int transform_R (const char *R_in, const char *jobspec, char **R_out);
    int pack_jobs (void *h, json_t *jobs);
    int allocate_jobs (void *h, bool use_alloced_queue);
    int recursive_get_slot_count (int *slot_count,
                                     json_t *curr_resource,
                                     json_error_t *error,
                                     bool *is_node_specified,
                                     int level);
    bool m_queue_depth_limit = false;
    job_map_iter m_iter;
};

}  // namespace detail
}  // namespace queue_manager
}  // namespace Flux

#endif  // QUEUE_POLICY_FCFS_HPP

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
