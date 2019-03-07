
#include "plugin/lundgren/join_strategies/bloom_join/bloom_filter.h"

#ifndef LUNDGREN_BLOOM_EXECUTOR
#define LUNDGREN_BLOOM_EXECUTOR



std::string generate_bloom_filter_string_from_query(std::string query) {


   bloom_parameters parameters;

   // How many elements roughly do we expect to insert?
   parameters.projected_element_count = 1000;

   // Maximum tolerable false positive probability? (0,1)
   parameters.false_positive_probability = 0.0001; // 1 in 10000

   // Simple randomizer (optional)
   parameters.random_seed = 0xA5A5A5A5;

   if (!parameters)
   {
      std::cout << "Error - Invalid set of bloom filter parameters!" << std::endl;
      return 1;
   }

   parameters.compute_optimal_parameters();

   //Instantiate Bloom Filter
   bloom_filter filter(parameters);

   // Insert into Bloom Filter
   {
      // Insert some numbers
      for (std::size_t i = 0; i < 100; ++i)
      {
         filter.insert(i);
      }
   }



    return NULL;
}


#endif  // LUNDGREN_BLOOM_EXECUTOR