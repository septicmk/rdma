#include <iostream>
#include <vector>
#include <cstdio>
#include "utils.hpp"
#include "verbs.hpp"

int main(int argc, char* argv[]){
    RDMA_Init(&argc, &argv);

    Verbs* vb = get_verb();
    mpi_env_t * mpi_env = get_mpi_env();
    int rank = mpi_env->rank;
    if (rank == 0){
        uint8_t *payload = (uint8_t *)malloc( 11*sizeof(uint8_t) );
        for(int i = 0 ; i < 10 ; ++i) payload[i] = 'a';
        vb->post_send(1, 1, payload, 10);
        vb->poll_cq(1, 1, 1);
    }else{
        vb->post_recv(1, 80);
        vb->poll_cq(1, 1, 0);
    }
    mpi_env->barrier();
    if(rank == 1){
       vb->DEBUG(1);
    }

    RDMA_Finalize();
    return 0;
}
