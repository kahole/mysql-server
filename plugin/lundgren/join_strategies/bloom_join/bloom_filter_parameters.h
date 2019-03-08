#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter.h"

#ifndef LUNDGREN_BLOOM_FILTER_PARAMETERS
#define LUNDGREN_BLOOM_FILTER_PARAMETERS

bloom_parameters get_bloom_parameters(uint64 expected_count);

bloom_parameters get_bloom_parameters(uint64 expected_count) {
    
    bloom_parameters parameters;
    // How many elements roughly do we expect to insert?
    parameters.projected_element_count = expected_count;
    // Maximum tolerable false positive probability? (0,1)
    parameters.false_positive_probability = 0.0001; // 1 in 10000
    // Simple randomizer (optional)
    parameters.random_seed = 0xA5A5A5A5;

    if (!parameters) {
        std::cout << "Error - Invalid set of bloom filter parameters!" << std::endl;
        return bloom_parameters();
    }

    parameters.compute_optimal_parameters();

    return parameters;
}


#endif  // LUNDGREN_BLOOM_FILTER_PARAMETERS