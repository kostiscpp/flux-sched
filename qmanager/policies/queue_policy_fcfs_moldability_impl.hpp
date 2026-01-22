/*****************************************************************************\
 * Copyright 2019 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef QUEUE_POLICY_FCFS_MOLDABILITY_IMPL_HPP
#define QUEUE_POLICY_FCFS_MOLDABILITY_IMPL_HPP

#include "qmanager/policies/queue_policy_fcfs_moldability.hpp"
#include "qmanager/policies/base/queue_policy_base.hpp"
#include <flux/core/job.h>
#include <flux/idset.h>

namespace Flux {
namespace queue_manager {
namespace detail {

////////////////////////////////////////////////////////////////////////////////
// Private Methods of Queue Policy FCFS
////////////////////////////////////////////////////////////////////////////////

template<class reapi_type>
std::tuple<int, int, int> queue_policy_fcfs_moldability_t<reapi_type>::selector_t::get_cores (void *h) 
{
    int rc = -1;
    flux_t *fh = (flux_t *)h;
    flux_future_t *f = NULL;
    char *all_ids, *core_ids;
    json_t *down_obj, *allocated_obj, *entry;
    size_t i;
    struct idset *all_ranks, *alloc_down_ranks = idset_decode (""), *core_ranks;
    if (!fh) {
        errno = EINVAL;
        return std::make_tuple(-1, -1, -1);
    }
    if (!(f = flux_rpc (fh, "resource.sched-status", NULL, 0, 0))) {
        return std::make_tuple(-1, -1, -1);
    }
    if ((rc = flux_rpc_get_unpack (f,
                                   "{s:{s:{s:[{s:s s:{s:s}}]}} s:{s:{s:o}} s:{s:{s:o}}}",
                                   "all",
                                   "execution",
                                   "R_lite",
                                   "rank",
                                   &all_ids,
                                   "children",
                                   "core",
                                   &core_ids,
                                   "down",
                                   "execution",
                                   "R_lite",
                                   &down_obj,
                                   "allocated",
                                   "execution",
                                   "R_lite",
                                   &allocated_obj
                                   ))
        < 0) {
        return std::make_tuple(-1, -1, -1);
    }
    json_array_foreach (down_obj, i, entry) {
        const char *rank = NULL;
        if (json_unpack (entry, "{s:s}", "rank", &rank) == 0) {
            idset_decode_add (alloc_down_ranks, rank, -1, NULL);
        }
    }
    json_array_foreach (allocated_obj, i, entry) {
        const char *rank = NULL;
        if (json_unpack (entry, "{s:s}", "rank", &rank) == 0) {
            idset_decode_add (alloc_down_ranks, rank, -1, NULL);
        }
    }

    all_ranks = idset_decode (all_ids);
    core_ranks = idset_decode (core_ids);
    // return all nodes count, used nodes and cores per node
    return std::make_tuple(idset_count (all_ranks), idset_count (alloc_down_ranks), idset_count (core_ranks));
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::selector_largest_fit_t::select(queue_policy_fcfs_moldability_t *policy,
                                                                                void *h,
                                                                                json_t *task_counts,
                                                                                json_t *durations,
                                                                                json_t *parallelism)  
{ 
    size_t n = json_array_size (task_counts);
    auto [all_nodes, used_nodes, cores_per_node] = this->get_cores (h);
    
    if (all_nodes == -1) return -1;
    int best_idx = -1;
    long long best_count = -1;
    // Track smallest as fallback if nothing fits
    int min_idx = 0;
    long long min_count = LLONG_MAX;
    for (size_t i = 0; i < n; i++) {
        json_t *tc = json_array_get (task_counts, i);
        if (!json_is_integer (tc))
            continue;
        long long count = json_integer_value (tc);
        // Ignore nonsensical entries
        if (count <= 0)
            continue;
        // record smallest for fallback
        if (count < min_count) {
            min_count = count;
            min_idx = (int)i;
        }
        // best fit: largest <= free_cores
        if (count <= (all_nodes - used_nodes)*cores_per_node && count > best_count) {
            best_count = count;
            best_idx = (int)i;
        }
    }
    // If nothing fit, fall back to smallest valid entry we saw
    if (best_idx < 0) {
        return (min_count == LLONG_MAX) ? -1 : min_idx;
    }
    return best_idx;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::selector_tanh_t::effective_task_count (void *h, json_t *parallelism, 
                                                                                           double load,
                                                                                           int max_task_count, 
                                                                                           int cores_per_node) 
{
    flux_log ((flux_t *)h, 0, "max_task_count= %d, cores_per_node= %d\n", max_task_count, cores_per_node);
    int cluster_size = 2 * ((max_task_count + cores_per_node - 1) / cores_per_node) * cores_per_node;
    flux_log ((flux_t *)h, 0, "cluster_size= %d\n", cluster_size);
    if (!parallelism || !json_is_number(parallelism))
        return -1;
    double p_app = json_number_value(parallelism);
    flux_log ((flux_t *)h, 0, "p_app=%f", p_app);
    double p_min = cluster_size * (1 - k) * std::pow ((p_app / cluster_size), delta);
    flux_log ((flux_t *)h, 0, "p_min=%f", p_min);
    double p_max = std::min(gamma * p_app * (1 - beta * load), (double)cluster_size);
    flux_log ((flux_t *)h, 0, "p_max=%f", p_max);
    double p_eff = p_min + (p_max - p_min) * (1 + std::tanh(b * (load_breakpoint - load))) / 2;
    flux_log ((flux_t *)h, 0, "p_eff=%f", p_eff);
    double scale = 1;
    if (scale * p_eff < 0) 
        return 0;
    else if (scale * p_eff > cluster_size)
        return cluster_size;
    return std::round(scale * p_eff); 
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::selector_tanh_t::select (queue_policy_fcfs_moldability_t *policy,
                                                                          void *h,
                                                                          json_t *task_counts,
                                                                          json_t *durations,
                                                                          json_t *parallelism)  
{
    double load = 0;
    int best_task_count;
    for(auto [_,x]: policy->m_pending) {
        json_error_t jerr;
        json_t *jobspec_obj = json_loads (policy->m_jobs[x]->jobspec.c_str (), 0, &jerr);
        json_t *resources_obj_x;
        json_t *system_obj_x;
        json_t *task_counts_x;
        size_t m = 0;

        if (json_unpack_ex (jobspec_obj,
                            &jerr,
                            0,
                            "{s:{s:o} s:o}",
                            "attributes",
                            "system",
                            &system_obj_x,
                            "resources",
                            &resources_obj_x) == 0)
        {
            if ((task_counts_x = json_object_get (system_obj_x, "task_counts"))
                && json_is_array (task_counts_x)) 
            {
                size_t n = json_array_size (task_counts_x);
                std::vector<long long> v;
                for (size_t i = 0; i < n; i++) {
                    json_t *tc = json_array_get (task_counts_x, i);
                    if (!json_is_integer (tc))
                        continue;
                    long long count = json_integer_value (tc);
                    // Ignore nonsensical entries
                    if (count <= 0)
                        continue;
                    v.push_back(count);
                }
                
                m = v.size();
                if (m > 0) {
                    size_t mid = m / 2;
                    std::nth_element(v.begin(), v.begin() + mid, v.end());
                    if (m % 2 == 1) {
                        load += v[mid];
                    } else {
                        long long upper = v[mid];
                        std::nth_element(v.begin(), v.begin() + mid - 1, v.begin() + mid);
                        long long lower = v[mid - 1];
                        load += (lower + upper) / 2.0;
                    }
                }
            }
            if (m == 0) {
                int total_slots;
                bool is_node_specified;
                policy->recursive_get_slot_count (&total_slots,
                                                  resources_obj_x,
                                                  &jerr,
                                                  &is_node_specified,
                                                  0);
                load += total_slots > 0 ? total_slots : 0;
            } 
        }
        
    }
    auto [all_nodes, used_nodes, cores_per_node] = this->get_cores (h);
    load = (used_nodes * cores_per_node + load) / (all_nodes * cores_per_node);
    if (load > 1.0) load = 1.0;
    flux_log((flux_t *)h, 0, "load=%f", load);
    long long max_task_count = 1;
    ssize_t n = json_array_size (task_counts);
    for (size_t i = 0; i < n; i++) {
        json_t *tc = json_array_get (task_counts, i);
        if (!json_is_integer (tc))
            continue;
        long long count = json_integer_value (tc);
        // Ignore nonsensical entries
        if (count <= 0)
            continue;
        // record smallest for fallback
        if (max_task_count < count) 
            max_task_count = count;
    }

    if((best_task_count = effective_task_count (h, parallelism, load, max_task_count, cores_per_node)) < 0) {
        return 0;
    }
    flux_log ((flux_t *)h, 0, "%d\n", best_task_count);
    int closest_task_count = INT_MAX, best_i = -1;
    for (size_t i = 0; i < n; i++) {
        json_t *tc = json_array_get (task_counts, i);
        if (!json_is_integer (tc))
            continue;
        long long count = json_integer_value (tc);
        // Ignore nonsensical entries
        if (count <= 0)
            continue;
        // record smallest for fallback
        flux_log ((flux_t *)h, 0, "count = %d\n", count);
        flux_log ((flux_t *)h, 0, "closest_task_count = %d\n", closest_task_count);
        flux_log ((flux_t *)h, 0, "best_task_count = %d\n", best_task_count);
        if (std::abs (count - best_task_count) < std::abs (closest_task_count - best_task_count)) {
            closest_task_count = count;
            best_i = (int)i;
            flux_log ((flux_t *)h, 0, "closest: %d at : %d\n", closest_task_count, best_i);
        }
    }

    return best_i;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::select_from (void *h, json_t *task_counts, json_t *durations, json_t *parallelism) 
{
    if (!m_selector)
        m_selector = std::make_unique<selector_largest_fit_t>();
    return m_selector->select (this, h, task_counts, durations, parallelism);
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::pack_jobs (void *h, json_t *jobs)
{
    unsigned int qd = 0;
    std::shared_ptr<job_t> job;
    auto iter = m_pending.begin ();
    while (iter != m_pending.end () && qd < m_queue_depth) {
        json_t *jobdesc;
        job = m_jobs[iter->second];

        // Unpack jobspec and choose walltime and task count if necessary
        json_error_t jerr;
        json_t *jobspec_obj = json_loads (job->jobspec.c_str (), 0, &jerr);
        json_t *resources_obj;
        json_t *system_obj;
        json_t *tasks_obj;
        json_t *task_counts;
        json_t *durations;
        int idx;

        if (json_unpack_ex (jobspec_obj,
                            &jerr,
                            0,
                            "{s:{s:o} s:o s:o}",
                            "attributes",
                            "system",
                            &system_obj,
                            "resources",
                            &resources_obj,
                            "tasks",
                            &tasks_obj
                            ) == 0
            && (task_counts = json_object_get (system_obj, "task_counts"))
            && (durations = json_object_get (system_obj, "durations"))
            && json_is_array (task_counts) 
            && json_is_array (durations) 
            && json_is_array (resources_obj)
            && (idx = select_from (h, task_counts, durations, json_object_get (system_obj, "parallelism"))) >= 0) 
        {
            json_t *count = json_array_get (task_counts, idx);
            json_t *duration = json_array_get (durations, idx);
            json_t* res0 = json_array_get (resources_obj, 0);
            if (!json_is_object (res0)) {
                // malformed jobspec (should not be able to be reached)
                json_decref (jobs);
                errno = EINVAL;
                return -1;
            }
            json_t* task0 = json_array_get (tasks_obj, 0);
            if (!json_is_object (task0)) {
                // malformed jobspec (should not be able to be reached)
                json_decref (jobs);
                errno = EINVAL;
                return -1;
            }

            auto [all_nodes, used_nodes, cores_per_node] = m_selector->get_cores (h);
            int node_count = (json_integer_value (count) + cores_per_node - 1) / cores_per_node;
            json_t *new_res0 = json_pack_ex (
                &jerr, 0, 
                "{s:s,s:i,s:[{s:s,s:i,s:[{s:s,s:i}],s:s}],s:b}",
                "type", "node", "count",  node_count, "with", 
                "type", "slot", "count",  std::min((int)json_integer_value (count), (int)cores_per_node), "with",
                "type", "core", "count", 1,
                "label", "task",
                "exclusive", true
            );
            json_array_set_new (resources_obj, 0, new_res0);                      
            json_object_set_new (system_obj, "duration", json_incref(duration));
            json_object_set_new (task0, "count", json_pack_ex (&jerr, 0, "{s:i}", "total", (int)json_integer_value (count)));
        }

        job->jobspec = std::string (json_dumps (jobspec_obj, JSON_COMPACT));
        flux_log ((flux_t *)h, 0, "%s\n", job->jobspec.c_str());
        json_decref (jobspec_obj);
        if (!(jobdesc =
                  json_pack ("{s:I s:s}", "jobid", job->id, "jobspec", job->jobspec.c_str ()))) {
            json_decref (jobs);
            errno = ENOMEM;
            return -1;
        }
        if (json_array_append_new (jobs, jobdesc) < 0) {
            json_decref (jobs);
            errno = ENOMEM;
            return -1;
        }
        m_jobs[iter->second] = job;
        iter++;
        qd++;
    }
    if (qd == m_queue_depth && m_pending.size () != m_queue_depth)
        m_queue_depth_limit = true;

    return 0;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::allocate_jobs (void *h, bool use_alloced_queue)
{
    json_t *jobs = nullptr;
    char *jobs_str = nullptr;
    job_map_iter iter;

    // move jobs in m_pending_provisional queue into
    // m_pending. Note that c++11 doesn't have a clean way
    // to "move" elements between two std::map objects so
    // we use copy for the time being.
    m_pending.insert (m_pending_provisional.begin (), m_pending_provisional.end ());
    m_pending_provisional.clear ();
    m_iter = m_pending.begin ();
    if (m_pending.empty ())
        return 0;
    if (!(jobs = json_array ())) {
        errno = ENOMEM;
        return -1;
    }

    if (pack_jobs (h, jobs) < 0)
        return -1;

    set_sched_loop_active (true);
    if (!(jobs_str = json_dumps (jobs, JSON_INDENT (0)))) {
        errno = ENOMEM;
        json_decref (jobs);
        return -1;
    }
    json_decref (jobs);
    if (reapi_type::match_allocate_multi (h, false, jobs_str, this) < 0) {
        free (jobs_str);
        set_sched_loop_active (false);
        return -1;
    };
    free (jobs_str);
    return 0;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::recursive_get_slot_count (int *slot_count,
                                                                            json_t *curr_resource,
                                                                            json_error_t *error,
                                                                            bool *is_node_specified,
                                                                            int level)
{
    size_t index;
    json_t *value;
    const char *type;
    json_t *count;
    json_t *with;
    if (level == 0 ) {
        *slot_count = -1;
        *is_node_specified = false;
    }
    if (json_array_size (curr_resource) == 0) {
        return -1;
    }
    json_array_foreach (curr_resource, index, value) {
        with = NULL;
        if (json_unpack_ex (value,
                            error,
                            0,
                            "{s:s s:o s?o}",
                            "type", &type,
                            "count", &count,
                            "with", &with) < 0) {
            return -1;
        }
        if (strcmp (type, "slot") == 0) {
            if (*slot_count > 0) {
                return (*slot_count = -1);
            }
            if (!json_is_integer (count)) {
                return -1;
            }
            return (*slot_count = json_integer_value (count));
        }
        if (strcmp (type, "node") == 0) {
            if (*is_node_specified) {
                return (*slot_count = -1);
            }
            *is_node_specified = true;
        }
        if (with) {
            recursive_get_slot_count (slot_count,
                                      with,
                                      error,
                                      is_node_specified,
                                      level+1);
        }
    }
    return (*slot_count);
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::transform_R (const char *R_in, const char *jobspec, char **R_out)
{
    json_error_t jerr;
    json_t* R_obj;
    json_t* jobspec_obj;
    json_t* resources;
    json_t* exec;
    bool is_node_specified;
    int total_slots = 0;
    size_t i;

    if(!(R_obj = json_loads (R_in, 0, &jerr))) {
        return -1;
    }
    if (!(jobspec_obj = json_loads (jobspec, 0, &jerr))) {
        json_decref (R_obj);
        return -1;
    }
    if (json_unpack_ex (jobspec_obj,
                            &jerr,
                            0,
                            "{s:[{s:{s:i}}]}",
                            "tasks",
                            "count",
                            "total",
                            &total_slots) != 0) {
        json_decref (jobspec_obj);
        json_decref (R_obj);
        return -1;
    }
    
    // if (recursive_get_slot_count (&total_slots,
    //                                   resources,
    //                                   &jerr,
    //                                   &is_node_specified,
    //                                   0) < 0) {
    //         json_decref (resources);
    //         json_decref (jobspec_obj);
    //         json_decref (R_obj);
    //         return -1;
    //     }

    if (!(exec = json_object_get (R_obj, "execution")) || !json_is_object(exec)) {
        json_decref (jobspec_obj);
        json_decref (R_obj);
        return -1;
    }
    
    json_object_set_new(exec, "nslots", json_integer(total_slots));
    if (!(*R_out = json_dumps(R_obj, JSON_COMPACT))) {
        json_decref (exec);
        json_decref(jobspec_obj);
        json_decref(R_obj);
        return -1;
    }

    json_decref (exec);
    json_decref(jobspec_obj);
    json_decref(R_obj);
    return 0;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::handle_match_success (flux_jobid_t jobid,
                                                           const char *status,
                                                           const char *R,
                                                           int64_t at,
                                                           double ov)
{
    char *R_final;

    if (!is_sched_loop_active ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<job_t> job = m_jobs[m_iter->second];
    if (job->id != static_cast<flux_jobid_t> (jobid)) {
        errno = EINVAL;
        return -1;
    }

    if (transform_R (R, job->jobspec.c_str(), &R_final) < 0) {
        errno = EINVAL;
        return -1;
    }

    job->schedule.reserved = std::string ("RESERVED") == status ? true : false;
    job->schedule.R = R_final;
    job->schedule.at = at;
    job->schedule.ov = ov;
    m_iter = to_running (m_iter, true);
    return 0;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::handle_match_failure (flux_jobid_t jobid, int errcode)
{
    if (!is_sched_loop_active ()) {
        errno = EINVAL;
        return -1;
    }
    if (errcode != EBUSY && errcode != ENODATA) {
        m_iter = to_rejected (m_iter, (errcode == ENODEV) ? "unsatisfiable" : "match error");
    }
    // Either:
    // ENODEV: the job is unsatisfiable, or
    // ENODATA && m_queue_depth_limit: the scheduling loop is being
    //     terminated per queue_depth_limit
    // We need to stop the sched loop, but the remainder is still schedulable.
    if (errcode == ENODEV) {
        set_schedulability (true);
    } else if (errcode == ENODATA && m_queue_depth_limit) {
        set_schedulability (true);
        m_queue_depth_limit = false;
    }
    set_sched_loop_active (false);
    // whatever happened here, a job transition has occurred, we need to run the
    // post_sched_loop
    m_scheduled = true;
    return 0;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::cancel (void *h,
                                             flux_jobid_t id,
                                             const char *R,
                                             bool noent_ok,
                                             bool &full_removal)
{
    return reapi_type::cancel (h, id, R, noent_ok, full_removal);
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::cancel (void *h, flux_jobid_t id, bool noent_ok)
{
    return reapi_type::cancel (h, id, noent_ok);
}

////////////////////////////////////////////////////////////////////////////////
// Public API of Queue Policy FCFS
////////////////////////////////////////////////////////////////////////////////

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::apply_params ()
{
    int rc = queue_policy_base_t::apply_params ();
    try {
        std::unordered_map<std::string, std::string>::const_iterator i;
        if ((i = m_pparams.find ("moldability-policy")) != m_pparams.end ()) {
            if (i->second == "\"largest_fit\"" || i->second == "\"tanh\"") {
                if (i->second == "\"tanh\"") {
                    m_selector_name = "tanh";
                    m_selector = std::make_unique<selector_tanh_t>();
                }
                else {
                    m_selector_name = "largest_fit";
                    m_selector = std::make_unique<selector_largest_fit_t>();
                }
            } else {
                rc += -1;
                errno = EINVAL;
            }
        } else {
            m_selector = std::make_unique<selector_largest_fit_t> ();
        }
    } catch (const std::invalid_argument &e) {
        rc = -1;
        errno = EINVAL;
    }
    return rc;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::run_sched_loop (void *h, bool use_alloced_queue)
{
    if (is_sched_loop_active ())
        return 1;
    int rc = 0;
    set_schedulability (false);
    rc += allocate_jobs (h, use_alloced_queue);
    return rc;
}

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::reconstruct_resource (void *h,
                                                           std::shared_ptr<job_t> job,
                                                           std::string &R_out)
{
    return reapi_type::update_allocate (h,
                                        job->id,
                                        job->schedule.R,
                                        job->schedule.at,
                                        job->schedule.ov,
                                        R_out);
}

}  // namespace detail
}  // namespace queue_manager
}  // namespace Flux

#endif  // QUEUE_POLICY_FCFS_IMPL_HPP

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
