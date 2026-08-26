/*****************************************************************************\
 * Copyright 2025 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

extern "C" {
#if HAVE_CONFIG_H
#include "config.h"
#endif
#include <errno.h>
}

#include "resource/traversers/dfu_flexible_largest_fit.hpp"
#include "resource/config/system_defaults.hpp"

using namespace Flux::Jobspec;
using namespace Flux::resource_model;
using namespace Flux::resource_model::detail;

dfu_flexible_largest_fit_t::dfu_flexible_largest_fit_t () = default;
dfu_flexible_largest_fit_t::dfu_flexible_largest_fit_t (std::shared_ptr<resource_graph_db_t> db,
                                std::shared_ptr<dfu_match_cb_t> m)
    : dfu_flexible_t (db, m)
{
}
dfu_flexible_largest_fit_t::dfu_flexible_largest_fit_t (const dfu_flexible_largest_fit_t &o) = default;
dfu_flexible_largest_fit_t &dfu_flexible_largest_fit_t::operator= (const dfu_flexible_largest_fit_t &o) = default;
dfu_flexible_largest_fit_t::dfu_flexible_largest_fit_t (dfu_flexible_largest_fit_t &&o) = default;
dfu_flexible_largest_fit_t &dfu_flexible_largest_fit_t::operator= (dfu_flexible_largest_fit_t &&o) = default;
dfu_flexible_largest_fit_t::~dfu_flexible_largest_fit_t () = default;

unsigned int dfu_flexible_largest_fit_t::total_slot_count (
    const std::vector<Resource> &resources,
    unsigned int parent_count) const
{
    unsigned int total = 0;

    for (const auto &resource : resources) {
        const unsigned int count =
            m_match->calc_effective_max (resource);

        if (resource.type == slot_rt && m_task_labels.find (resource.label) != m_task_labels.end ()) {
            total += parent_count * count;
            continue;
        }

        total += total_slot_count (
            resource.with,
            parent_count * count);
    }

    return total;
}

const std::string *dfu_flexible_largest_fit_t::find_task_label (
    const std::vector<Resource> &resources) const
{
    for (const auto &resource : resources) {
        if (resource.type == slot_rt
            && m_task_labels.find (resource.label) != m_task_labels.end ())
            return &resource.label;

        if (const auto *label = find_task_label (resource.with))
            return label;
    }

    return nullptr;
}

std::vector<dfu_flexible_largest_fit_t::variant_index_t>
dfu_flexible_largest_fit_t::extract_variant_indices (
    const std::vector<std::vector<Resource>> &resources) const
{
    std::unordered_map<std::string, std::size_t> next_offset;
    std::vector<variant_index_t> result;

    result.reserve (resources.size ());
    for (std::size_t i = 0; i < resources.size (); ++i) {
        const auto *label = find_task_label (resources[i]);
        if (!label) {
            errno = EINVAL;
            return {};
        }
        result.push_back ({
            i,
            *label,
            next_offset[*label]++
        });
    }
    return result;
}

int dfu_flexible_largest_fit_t::select (std::vector<Jobspec::Resource> &resources,
                            vtx_t root,
                            jobmeta_t &meta,
                            bool excl)
{
    auto xor_resources = split_xor_slots (resources);

    if (xor_resources.empty ()) {
        m_err_msg += __FUNCTION__;
        m_err_msg += ": split_xor_slots failed.\n";
        if (errno != 0) {
            m_err_msg += strerror (errno);
            m_err_msg += "\n";
        }
        return -1;
    }

    if (xor_resources.size() == 1) {
        if (dfu_impl_t::select (xor_resources[0], root, meta, excl) == 0) {
            // Success - update the passed resources to the matching variant
            resources = xor_resources[0];
            return 0;
        } else return -1;
    }


    auto variants = extract_variant_indices (xor_resources);

    std::sort (
        variants.begin (),
        variants.end (),
        [this, &xor_resources] (const auto &lhs, const auto &rhs) {
            return total_slot_count (xor_resources[lhs.resource_idx])
                 > total_slot_count (xor_resources[rhs.resource_idx]);
        });

    const auto original_duration = meta.duration;

    for (const auto &variant : variants) {
         meta.duration = original_duration;

        const auto duration_it =
            m_durations.find (variant.task_label);

        if (duration_it != m_durations.end ()) {
            const auto &durations = duration_it->second;

            if (variant.duration_offs >= durations.size ()) {
                m_err_msg += __FUNCTION__;
                m_err_msg += ": duration offset out of range for task label ";
                m_err_msg += variant.task_label;
                m_err_msg += ".\n";

                errno = EINVAL;
                meta.duration = original_duration;
                return -1;
            }

            meta.duration =
                durations[variant.duration_offs];
        }

        auto &candidate =
            xor_resources[variant.resource_idx];
        if (dfu_impl_t::select (candidate, root, meta, excl) == 0) {
            // Success - update the passed resources to the matching variant
            resources = std::move (candidate);
            return 0;
        }
    }

    return -1;
}

std::vector<std::vector<Resource>> dfu_flexible_largest_fit_t::split_xor_slots (
    const std::vector<Resource> &resources) const
{
    // Start with one empty variant and expand it as each sibling resource
    // contributes either a single normalized subtree or multiple xor choices.
    std::vector<std::vector<Resource>> base_variants (1);
    std::vector<Resource> xor_options;

    for (const auto &resource : resources) {
        // Normalize nested xor_slot descendants before combining this sibling
        // with the variants accumulated so far.
        auto child_variants = split_xor_slots (resource.with);

        // Check if recursive expansion failed
        if (child_variants.empty ()) {
            return {};
        }

        if (resource.type == xor_slot_rt) {
            // xor_slot siblings are alternatives: convert each expanded child
            // into a normal slot option and defer combining until the end.
            for (const auto &child_variant : child_variants) {
                Resource option = resource;
                option.type = slot_rt;
                option.with = child_variant;
                xor_options.push_back (option);

                // Check if xor_options collection exceeds the limit
                if (exceeds_max_expansion (xor_options.size ()))
                    return {};
            }

            continue;
        }

        std::vector<std::vector<Resource>> next_variants;
        for (const auto &variant : base_variants) {
            for (const auto &child_variant : child_variants) {
                // Non-xor resources are required together, so build the
                // cross-product of prior siblings with each child expansion.
                Resource expanded = resource;
                expanded.with = child_variant;

                auto next = variant;
                next.push_back (expanded);
                next_variants.push_back (std::move (next));

                // Check if expansion exceeds the limit
                if (exceeds_max_expansion (next_variants.size ()))
                    return {};
            }
        }

        base_variants = std::move (next_variants);
    }

    if (xor_options.empty ())
        return base_variants;

    std::vector<std::vector<Resource>> results;
    for (const auto &variant : base_variants) {
        for (const auto &option : xor_options) {
            // Attach exactly one xor choice to each fully expanded required
            // sibling set to produce the final candidate jobspec resources.
            auto next = variant;
            next.push_back (option);
            results.push_back (std::move (next));

            // Check if final expansion exceeds the limit
            if (exceeds_max_expansion (results.size ()))
                return {};
        }
    }

    return results;
}
/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
