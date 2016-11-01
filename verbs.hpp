#ifndef __VERBS_H__
#define __VERBS_H__

#include <infiniband/verbs.h>
#include <iostream>
#include <cstdint>
#include <vector>
#include "mpi.h"
#include "utils.hpp"

class mpi_env_t {
public:
	int rank;
	int size;
	MPI_Comm comm;

    void mpi_env_init(int *argc, char** argv[]);
    void mpi_env_finalize();
    void barrier();
};

class C_Ctl;
class UD_Ctl;

class Verbs{
private:
    /* device */
    ibv_device * device;
    const char * device_name;
    uint64_t device_guid;
    ibv_device_attr device_attr;

    /* port */
    uint8_t port;
    ibv_port_attr port_attr;

    /* context && protection_domain */
    ibv_context * context;
    ibv_pd * protection_domain;
    

    /* QP ctl */
    C_Ctl* c_ctl;
    UD_Ctl* ud_ctl;
   
public:
    Verbs()=delete;
    Verbs(int num_conn_qps, int num_dgram_qps,
          int conn_send_buf_size, int conn_recv_buf_size,
          int dgram_senf_buf_size, int dgram_recv_buf_size );

    int get_rank();
    int get_size();

    void init_device( const char* target_device_name="mlx4_0", uint8_t target_port=1 );
    void finalize();
    
    void post_send(int use_ud, int rank, uint8_t* msg, int size);
    void post_recv(int use_ud, int size);
    void poll_cq(int use_ud, int num, int x);
    void DEBUG(int use_ud);
};

class C_Ctl{
private:
    /* config */
    const uint32_t DFT_Q_DEPTH = 128;
    const uint32_t DFT_MAX_INLINE = 64;
    const uint32_t DFT_PSN = 0;
    const uint32_t DFT_MAX_DEST_RD_ATOMIC = 16;
    const uint32_t DFT_MAX_RD_ATOMIC = 16;
    const uint32_t DFT_MIN_RNR_TIMER = 0x12;
    const uint32_t DFT_TIMEOUT = 14;
    const uint32_t DFT_RETRY_CNT = 7;
    const uint32_t DFT_RNR_RETRY = 7;


    /* context and protection domain*/
    struct ibv_context * context;
    struct ibv_pd * protection_domain;
    uint8_t port;
    ibv_port_attr port_attr;

    /* conn_qp */
    int num_qps;
    struct ibv_cq ** cq;
    struct ibv_qp ** qp;
    int recv_buf_size, send_buf_size;
    uint8_t *recv_buf, *send_buf;
    struct ibv_mr *recv_buf_mr, *send_buf_mr;
    int offset, head, tail, dispatch;

    /* remote qp info */
    int num_remote_qps;
    struct remote_qp_attr_t* remote_qps;

    void _create_qps();
    void _alloc_qp_buf();
    void _exchange_remote_qp_info();
    void __connect_qps(int i, struct remote_qp_attr_t *remote_qp_attr);
    void _connect_qps();

public:
    C_Ctl()=delete;
    C_Ctl(struct ibv_context* ctx, struct ibv_pd* pd, uint8_t port, struct ibv_port_attr port_attr)
        : context(ctx)
        , protection_domain(pd)
        , port(port)
        , port_attr(port_attr){}
    
    void init(int num_conn_qps, int send_buf_size, int recv_buf_size);
    void finalize();

    void post_RDMA_write(int rank, uint8_t* msg, int size);
    void poll_cq(int num, int rank);

    void DEBUG();
};

class UD_Ctl{
private:
    /* config */
    const uint32_t DFT_Q_DEPTH = 128;
    const uint32_t DFT_MAX_INLINE = 64;
    const uint32_t DFT_PSN = 0;
    const uint32_t DFT_MIN_RNR_TIMER = 0x12;
    const uint32_t DFT_TIMEOUT = 14;
    const uint32_t DFT_RETRY_CNT = 7;
    const uint32_t DFT_RNR_RETRY = 7;
    const uint32_t DFT_QKEY = 0x80000000;

    /* context and protection domain*/
    struct ibv_context * context;
    struct ibv_pd * protection_domain;
    uint8_t port;
    ibv_port_attr port_attr;

    /* dgram_qp */
    int num_qps;
    struct ibv_cq ** send_cq;
    struct ibv_cq ** recv_cq;
    struct ibv_qp ** qp;
    struct ibv_ah ** ah;
    int recv_buf_size, send_buf_size;
    uint8_t *recv_buf, * send_buf;
    struct ibv_mr * recv_buf_mr, * send_buf_mr;
    int offset, head, tail, dispatch;

    /* remote qp info */
    int num_remote_qps;
    struct remote_qp_attr_t * remote_qps;

    void _create_qps();
    void _alloc_qp_buf();
    void _exchange_remote_qp_info();
    void _build_ah();

public:
    UD_Ctl()=delete;
    UD_Ctl(struct ibv_context* ctx, struct ibv_pd* pd, uint8_t port, struct ibv_port_attr port_attr)
        : context(ctx)
        , protection_domain(pd)
        , port(port)
        , port_attr(port_attr) {}


    void init(int num_dgram_qps, int send_buf_size, int recv_buf_size);
    void finalize();

    void post_UD_send(int rank, uint8_t* msg, int size);
    void post_UD_recv(int size);
    void poll_cq(int num, int send);

    void DEBUG();
};


extern mpi_env_t* mpi_env;
extern Verbs *global_verbs;
void RDMA_Init(int* argc, char** argv[]);
void RDMA_Finalize();

Verbs* get_verb();
mpi_env_t * get_mpi_env();

#endif

