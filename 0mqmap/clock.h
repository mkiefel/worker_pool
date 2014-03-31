#ifndef _CLOCK_7B3C1C34C7D3_
#define _CLOCK_7B3C1C34C7D3_

#include <chrono>

#define GCC_VERSION (__GNUC__ * 10000 \
    + __GNUC_MINOR__ * 100 \
    + __GNUC_PATCHLEVEL__)


namespace zmqmap {

#if GCC_VERSION < 40700
typedef std::chrono::monotonic_clock steadyclock_type;
#else
typedef std::chrono::steady_clock steadyclock_type;
#endif

}

#endif /* _CLOCK_7B3C1C34C7D3_ */
