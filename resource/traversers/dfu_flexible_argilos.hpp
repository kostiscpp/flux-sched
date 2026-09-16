/*****************************************************************************\
 * Copyright 2025 Lawrence Livermore National Security, LLC
 * (c.f. AUTHORS, NOTICE.LLNS, LICENSE)
 *
 * This file is part of the Flux resource manager framework.
 * For details, see https://github.com/flux-framework.
 *
 * SPDX-License-Identifier: LGPL-3.0
\*****************************************************************************/

#ifndef DFU_FLEXIBLE_ARGILOS_HPP
#define DFU_FLEXIBLE_ARGLIOS_HPP

#include "resource/traversers/dfu_flexible.hpp"

using namespace Flux::resource_model::detail;
namespace Flux {
namespace resource_model {

class dfu_flexible_argilos_t : public dfu_flexible_t {
    // struct to convert map of resources counts to an index
   public:

    unsigned int total_slot_count (
        const std::vector<Jobspec::Resource> &resources,
        unsigned int parent_count = 1) const;

    const std::string *find_task_label (
        const std::vector<Jobspec::Resource> &resources) const;

    struct variant_index_t {
        std::size_t resource_idx;
        std::string task_label;
        std::size_t duration_offs;
    };
    
    std::vector <variant_index_t> extract_variant_indices (
        const std::vector<std::vector<Jobspec::Resource>> &resources) const;

    int effective_task_count (double parallelism,
                              double load,
                              int max_task_count,
                              int cores_per_node);
    std::tuple<int, int, int> get_status ();
    
    int select (std::vector<Jobspec::Resource> &resources, vtx_t root, jobmeta_t &meta, bool excl);

    std::vector<std::vector<Jobspec::Resource>> split_xor_slots (
        const std::vector<Jobspec::Resource> &resources) const;

   public:
    dfu_flexible_argilos_t ();
    dfu_flexible_argilos_t (std::shared_ptr<resource_graph_db_t> db, std::shared_ptr<dfu_match_cb_t> m);
    dfu_flexible_argilos_t (const dfu_flexible_argilos_t &o);
    dfu_flexible_argilos_t (dfu_flexible_argilos_t &&o);
    dfu_flexible_argilos_t &operator= (const dfu_flexible_argilos_t &o);
    dfu_flexible_argilos_t &operator= (dfu_flexible_argilos_t &&o);
    ~dfu_flexible_argilos_t ();

   private:
    double b = 0.8;
    double beta = 0.7;
    double k = 0.8;
    double load_breakpoint = 0.8;
    double delta = 0.9;
    double gamma = 2.0;
};

}  // namespace resource_model
}  // namespace Flux

#endif  // DFU_TRAVERSE_HPP

/*
 * vi:tabstop=4 shiftwidth=4 expandtab
 */
