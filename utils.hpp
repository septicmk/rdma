#ifndef __UTILS_H__
#define __UTILS_H__

#include <iostream>
#include <cstdio>
#include <mpi.h>

//#define USE_RC true
#define USE_UC true

struct remote_qp_attr_t {
    // Info about RDMA buffer associated with this QP
    uintptr_t rdma_buf;
    uint32_t rdma_buf_size;
    uint32_t rkey;
    // extra parameters needs to be exchanged when connetting QPs
    uint16_t lid;
    uint32_t qpn;
};


#define LOG(...) fprintf(stdout, __VA_ARGS__)

#define ASSERT( Predicate, Err_msg )    \
if(true){                               \
    if( !(Predicate) ){                 \
        std::cerr << "CHECK failed :"   \
            << Err_msg  << " at ("      \
            << __FILE__ << ", "         \
            << __LINE__ << ")"          \
            << std::endl;               \
		exit(1);						\
    }                                   \
}


#endif
