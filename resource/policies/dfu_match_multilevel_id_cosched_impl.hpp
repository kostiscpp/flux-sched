/*****************************************************************************\
 * Copyright 2021 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef DFU_MATCH_MULTILEVEL_COSCHED_IMPL_HPP
#define DFU_MATCH_MULTILEVEL_COSCHED_IMPL_HPP

#include <numeric>
#include "resource/policies/dfu_match_multilevel_id_cosched.hpp"

namespace Flux {
namespace resource_model {

////////////////////////////////////////////////////////////////////////////////
// MultiLevel ID Match Policy Class's Private Method Definitions
////////////////////////////////////////////////////////////////////////////////

template<typename FOLD>
multilevel_id_cosched_t<FOLD>::score_factor_t::score_factor_t (resource_type_t type,
                                                       unsigned add_by,
                                                       unsigned multiple_by)
    : m_type (type), m_add_by (add_by), m_multiply_by (multiple_by)
{
}

template<typename FOLD>
int64_t multilevel_id_cosched_t<FOLD>::score_factor_t::calc_factor (int64_t base_factor, int64_t break_tie)
{
    int64_t add, mul, tie;
    if (base_factor < 0 || break_tie < 0) {
        errno = EINVAL;
        return -1;
    }

    if (base_factor > (std::numeric_limits<int64_t>::max () - m_add_by)) {
        errno = EOVERFLOW;
        return -1;
    }
    add = base_factor + m_add_by;

    if (add > (std::numeric_limits<int64_t>::max () / m_multiply_by)) {
        errno = EOVERFLOW;
        return -1;
    }
    mul = add * m_multiply_by;
    tie = std::abs (break_tie % static_cast<int64_t> (m_multiply_by) - 1);

    if (mul > (std::numeric_limits<int64_t>::max () - tie)) {
        errno = EOVERFLOW;
        return -1;
    }
    m_factor = mul + tie;
    return m_factor;
}

////////////////////////////////////////////////////////////////////////////////
// MultiLevel ID Match Policy Class's Public Method Definitions
////////////////////////////////////////////////////////////////////////////////

template<typename FOLD>
multilevel_id_cosched_t<FOLD>::multilevel_id_cosched_t ()
{
}

template<typename FOLD>
multilevel_id_cosched_t<FOLD>::multilevel_id_cosched_t (const std::string &name) : dfu_match_cb_t (name)
{
}

template<typename FOLD>
multilevel_id_cosched_t<FOLD>::multilevel_id_cosched_t (const multilevel_id_cosched_t &o) : dfu_match_cb_t (o)
{
    m_stop_on_k_matches = o.m_stop_on_k_matches;
    m_multilevel_scores = o.m_multilevel_scores;
    m_multilevel_factors = o.m_multilevel_factors;
}

template<typename FOLD>
multilevel_id_cosched_t<FOLD> &multilevel_id_cosched_t<FOLD>::operator= (const multilevel_id_cosched_t &o)
{
    dfu_match_cb_t::operator= (o);
    m_stop_on_k_matches = o.m_stop_on_k_matches;
    m_multilevel_scores = o.m_multilevel_scores;
    m_multilevel_factors = o.m_multilevel_factors;
    return *this;
}

template<typename FOLD>
multilevel_id_cosched_t<FOLD>::~multilevel_id_cosched_t ()
{
}

template<typename FOLD>
void multilevel_id_cosched_t<FOLD>::count_free_under_u (vtx_t u,
                                                        const resource_graph_t &g,
                                                        resource_type_t t,
                                                        std::unordered_map<int64_t, int64_t> &free)
                                                                               
{
    f_out_edg_iterator_t ei, ei_end;
    for (tie (ei, ei_end) = out_edges (u, g); ei != ei_end; ++ei) {
        vtx_t tgt = target (*ei, g);
        if (g[tgt].type == t && g[tgt].schedule.allocations.empty ()) {
            free[g[tgt].id % m_nwise]++;
            if (free[g[tgt].id % m_nwise] > m_best_free) {
                m_best_free = free[g[tgt].id % m_nwise];
                m_best_free_i = g[tgt].id % m_nwise;
                std::cout << "m_best_free_i_cfu =" << m_best_free_i << std::endl;
            }
        }
        count_free_under_u (tgt, g, t, free);
    }
}

template<typename FOLD>
int multilevel_id_cosched_t<FOLD>::dom_finish_graph (subsystem_t subsystem,
                                             const std::vector<Flux::Jobspec::Resource> &resources,
                                             const resource_graph_t &g,
                                             scoring_api_t &dfu)
{
    int64_t score = MATCH_MET;
    for (auto &resource : resources) {
        unsigned qc = dfu.qualified_count (subsystem, resource.type);
        unsigned count = calc_count (resource, qc);
        if (count == 0) {
            score = MATCH_UNMET;
            break;
        }
        dfu.choose_accum_best_k (subsystem, resource.type, count, m_comp);
    }
    dfu.set_overall_score (score);
    m_multilevel_scores = 0;
    return (score == MATCH_MET) ? 0 : -1;
}

template<typename FOLD>
int multilevel_id_cosched_t<FOLD>::dom_finish_slot (subsystem_t subsystem, scoring_api_t &dfu)
{
    std::vector<resource_type_t> types;
    dfu.resrc_types (subsystem, types);
    for (auto &type : types)
        dfu.choose_accum_all (subsystem, type, m_comp);
    return 0;
}

template<typename FOLD>
int multilevel_id_cosched_t<FOLD>::dom_discover_vtx (vtx_t u,
                                             subsystem_t subsystem,
                                             const std::vector<Flux::Jobspec::Resource> &resources,
                                             const resource_graph_t &g)
{
    if (m_multilevel_factors.find (g[u].type) != m_multilevel_factors.end ()) {
        if (g[u].id < -1)
            return -1;
        auto &f = m_multilevel_factors[g[u].type];
        int64_t factor = f.calc_factor (g[u].id + 1, g[u].uniq_id);
        if (factor < 0)
            return factor;
        m_multilevel_scores += factor;
    }
    if (g[u].type == socket_rt) {
        m_free_cores.clear();
        m_best_free = 0;
        m_best_free_i = 0;
        count_free_under_u (u, g, core_rt, m_free_cores);
        std::cout << "even = " << m_free_cores[0] << ", odd= " << m_free_cores[1] << std::endl;
        std::cout << m_best_free << ", " << m_best_free_i << std::endl;
    }
    incr ();
    return 0;
}

template<typename FOLD>
int multilevel_id_cosched_t<FOLD>::dom_finish_vtx (vtx_t u,
                                           subsystem_t subsystem,
                                           const std::vector<Flux::Jobspec::Resource> &resources,
                                           const resource_graph_t &g,
                                           scoring_api_t &dfu)
{
    int64_t score = MATCH_MET;
    int64_t overall;
    int64_t score_u;

    for (auto &resource : resources) {
        if (resource_type_t{resource.type} != g[u].type)
            continue;
        // Jobspec resource type matches with the visiting vertex
        for (auto &c_resource : resource.with) {
            // Test children resource count requirements
            unsigned qc = dfu.qualified_count (subsystem, c_resource.type);
            unsigned count = calc_count (c_resource, qc);
            if (count == 0) {
                score = MATCH_UNMET;
                break;
            }
            dfu.choose_accum_best_k (subsystem, c_resource.type, count, m_comp);
        }
    }
    // if (score == MATCH_MET)
    if (g[u].type == core_rt && m_core_id_start < 0) 
        m_core_id_start = g[u].id;

    auto factor = m_multilevel_factors.find (g[u].type);
    if (factor != m_multilevel_factors.end ())
        m_multilevel_scores -= factor->second.m_factor;
    if (score == MATCH_MET)
        if (g[u].type == core_rt) {
            if (g[u].id >= 0 && g[u].id % m_nwise == m_best_free_i)
                score_u = 1000000 + m_free_cores[g[u].id % m_nwise] + (g[u].id - m_core_id_start);
            else
                score_u = (g[u].id - m_core_id_start >= 0) ? m_free_cores[g[u].id % m_nwise] + (g[u].id - m_core_id_start) : 0;
            //score_u = (1000000 +  g[u].id) * (g[u].id % m_nwise);
        }
        else 
            score_u = g[u].id;
    if (score == MATCH_MET && g[u].type == socket_rt) {
        m_core_id_start = -1;
    }
    overall = (score == MATCH_MET) ? (score + m_multilevel_scores + score_u + 1) : score;
    dfu.set_overall_score (overall);
    decr ();
    return (score == MATCH_MET) ? 0 : -1;
}

template<typename FOLD>
int multilevel_id_cosched_t<FOLD>::set_stop_on_k_matches (unsigned k)
{
    if (k > 1)
        return -1;
    m_stop_on_k_matches = k;
    return 0;
}

template<typename FOLD>
int multilevel_id_cosched_t<FOLD>::get_stop_on_k_matches () const
{
    return m_stop_on_k_matches;
}

template<typename FOLD>
int multilevel_id_cosched_t<FOLD>::add_score_factor (resource_type_t type,
                                             unsigned add_by,
                                             unsigned multiple_by)
{
    try {
        if (m_multilevel_factors.find (type) != m_multilevel_factors.end ()) {
            errno = EEXIST;
            return -1;
        }
        auto ret = m_multilevel_factors.insert ({type, score_factor_t{type, add_by, multiple_by}});
        if (!ret.second) {
            errno = ENOMEM;
            return -1;
        }
        return 0;
    } catch (std::bad_alloc &e) {
        errno = ENOMEM;
        return -1;
    }
}

}  // namespace resource_model
}  // namespace Flux

#endif  // DFU_MATCH_MULTILEVEL_ID_COSCHED_IMPL_HPP

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
