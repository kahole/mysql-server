#include <string.h>

#ifndef LUNDGREN_HELPERS
#define LUNDGREN_HELPERS


std::string random_string(size_t length) {
  auto randchar = []() -> char {
    const char charset[] =
        //"0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    const size_t max_index = (sizeof(charset) - 1);
    return charset[rand() % max_index];
  };
  std::string str(length, 0);
  std::generate_n(str.begin(), length, randchar);
  return str;
}

std::string generate_interim_name() {
  return random_string(30);
}

#endif  // LUNDGREN_HELPERS
