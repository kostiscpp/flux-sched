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

namespace Flux {
namespace queue_manager {
namespace detail {

////////////////////////////////////////////////////////////////////////////////
// Private Methods of Queue Policy FCFS
////////////////////////////////////////////////////////////////////////////////

template<class reapi_type>
int queue_policy_fcfs_moldability_t<reapi_type>::pack_jobs (json_t *jobs)
{
    unsigned int qd = 0;
    std::shared_ptr<job_t> job;
    auto iter = m_pending.begin ();
    while (iter != m_pending.end () && qd < m_queue_depth) {
        json_t *jobdesc;
        job = m_jobs[iter->second];

        // Unpack jobspec and choose walltime and task count if necessary
        json_error_t jerr;
        json_t *jobspec_obj = json_loads(job->jobspec.c_str (), 0, &jerr);
        json_t *resources_obj;
        json_t *attributes_obj = json_object_get(jobspec_obj, "attributes");
        json_t *system_obj = json_object_get(attributes_obj, "system");
        json_t *task_counts;
        json_t *durations;
        if ((task_counts = json_object_get(system_obj, "task_counts")) && json_is_array(task_counts)) {
            json_t *count = json_array_get(task_counts, 0);
            resources_obj = json_object_get(jobspec_obj, "resources");
            if (json_is_array(resources_obj)) {
                json_t* res0 = json_array_get(resources_obj, 0);
                if (json_is_object(res0)) {
                    json_object_set_new(res0, "count", json_incref(count));                      
                }
            }
        }
        if ((durations = json_object_get(system_obj, "durations")) && json_is_array(durations)) {
            json_t *duration = json_array_get(durations, 0);
            json_object_set_new(system_obj, "duration", json_incref(duration));
        }
        job->jobspec = std::string(json_dumps(jobspec_obj, JSON_COMPACT));
        json_decref(jobspec_obj);
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
    if (pack_jobs (jobs) < 0)
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
int queue_policy_fcfs_moldability_t<reapi_type>::handle_match_success (flux_jobid_t jobid,
                                                           const char *status,
                                                           const char *R,
                                                           int64_t at,
                                                           double ov)
{
    json_t *R_obj = json_loads(R, 0, NULL);
    
    if (!is_sched_loop_active ()) {
        errno = EINVAL;
        return -1;
    }
    std::shared_ptr<job_t> job = m_jobs[m_iter->second];
    if (job->id != static_cast<flux_jobid_t> (jobid)) {
        errno = EINVAL;
        return -1;
    }
    
    json_t *jobspec_obj = json_loads(job->jobspec.c_str(), 0, NULL);
    int total_slots = 0;

    /* jobspec.resources is an array, e.g.:
     *   "resources": [ { "count": 2 }, { "count": 3 } ]
     */

    json_t *resources = json_object_get(jobspec_obj, "resources");
    if (resources && json_is_array(resources)) {
        size_t i;
        json_t *res;
        json_array_foreach(resources, i, res) {
            json_t *count = json_object_get(res, "count");
            if (count && json_is_integer(count))
                total_slots += json_integer_value(count);
        }
    }
    json_decref(jobspec_obj);
    json_t *exec = json_object_get(R_obj, "execution");
    json_object_set_new(exec, "nslots", json_integer(total_slots));
    R = json_dumps(R_obj, JSON_COMPACT);
    json_decref(R_obj);

    job->schedule.reserved = std::string ("RESERVED") == status ? true : false;
    job->schedule.R = R;
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
    return queue_policy_base_t::apply_params ();
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
