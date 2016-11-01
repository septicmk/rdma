#include <cstring>
#include <string>
#include <vector>
#include "verbs.hpp"
#include "utils.hpp"
#include <mpi.h>

Verbs::Verbs( int num_conn_qps
            , int num_dgram_qps
            , int conn_send_buf_size
            , int conn_recv_buf_size
            , int dgram_send_buf_size
            , int dgram_recv_buf_size){
    init_device();

    ASSERT(context!=NULL, "please open device first");
    ASSERT(protection_domain!=NULL, "please open device first");
    c_ctl = NULL; ud_ctl = NULL;
    if(num_conn_qps){
        c_ctl = new C_Ctl(context, protection_domain, port, port_attr);
        c_ctl->init(num_conn_qps, conn_send_buf_size, conn_recv_buf_size);
    }
    if(num_dgram_qps){
        ud_ctl = new UD_Ctl(context, protection_domain, port, port_attr);
        ud_ctl->init(num_dgram_qps, dgram_send_buf_size, dgram_recv_buf_size);
    }
}


/*
 * Get target device
 * Open context
 * Get device attribute
 * Allocate protection domain
 */
void Verbs::init_device( const char* target_device_name, uint8_t target_port){

    // 1. get device list
    int num_devices = 0;
    ibv_device ** devices = ibv_get_device_list( &num_devices );
    ASSERT( devices!=NULL, "no Verbs devices found!");

    // 2. get target device
    device = NULL;
    for(int i = 0; i < num_devices; ++i){
        if( 0 == strcmp(target_device_name, ibv_get_device_name(devices[i])) ){
            device = devices[i];
            device_name = ibv_get_device_name( device );
            device_guid = ibv_get_device_guid( device );
        }
	}
    ASSERT( device!=NULL, "Failed to find target device" );

    // 3. open context
    context = ibv_open_device( device );
    ASSERT( context!=NULL, "Failed to open target device context" );

    // 4. get device attr
    int retval = ibv_query_device( context, &device_attr );
    ASSERT( 0==retval, "Failed to get device attributes" );

    // 5. get port attr
    port = target_port;
    retval = ibv_query_port( context, port, &port_attr);
    ASSERT( 0==retval, "Failed to get port attributes" );

    // 6. create protection domain
    protection_domain = ibv_alloc_pd( context ); 
    ASSERT( protection_domain!=NULL, "Failed to alloc protection domain")

}


void C_Ctl::finalize(){
    /* clear alloc message buffer */
	if( recv_buf_mr ){
		int retval = ibv_dereg_mr( recv_buf_mr );
		ASSERT( 0==retval, "Failed to deregister conn recv buf" );
	}
	if( recv_buf ){
		free( recv_buf );
		recv_buf = NULL;
	}
	if( send_buf_mr ){
		int retval = ibv_dereg_mr( send_buf_mr );
		ASSERT( 0==retval, "Failed to deregister dgram send buf" );
	}
	if( send_buf ){
		free( send_buf );
		send_buf = NULL;
	}
	
	/* clear remote qp attr info */
	if( remote_qps ){
		free( remote_qps );
		remote_qps = NULL;
	}
	/* clear conn qp and cq */
    if( cq || qp ){
		for(int i = 0; i < num_qps; ++i){
			int retval = ibv_destroy_qp( qp[i] );
			ASSERT( 0==retval, "Failed to destroy conn qp ");
		}
		free(qp);
		for(int i = 0; i < num_qps; ++i){
			int retval = ibv_destroy_cq( cq[i] );
			ASSERT( 0==retval, "Failed to destroy conn cq ");
		}
		free(cq);
    }
}

void UD_Ctl::finalize(){
    /* free all prealloc buf*/
    if( recv_buf_mr ){
        int retval = ibv_dereg_mr( recv_buf_mr );
        ASSERT( 0==retval, "Failed to deregister dgram recv buf");
    }
    if( recv_buf ) {
        free( recv_buf );
        recv_buf = NULL;
    }
    if( send_buf_mr){
        int retval = ibv_dereg_mr( send_buf_mr );
        ASSERT( 0== retval, "Failed to deregister dgram send buf");
    }
    if( send_buf){
        free( send_buf );
        send_buf = NULL;
    }

	/* clear remote qp attr info */
    if( remote_qps ){
        free( remote_qps );
        remote_qps = NULL;
    }

    /* clear dgram qp and cq */
    if( qp || recv_cq || send_cq ){
        for(int i = 0 ; i < num_qps; ++i){
            int retval = ibv_destroy_qp( qp[i] );
            ASSERT( 0==retval, "Falied to destroy dgram qp");
            retval = ibv_destroy_cq( send_cq[i] );
            ASSERT( 0==retval, "Falied to destroy dgram send cq" );
            retval = ibv_destroy_cq( recv_cq[i] );
            ASSERT( 0==retval, "Falied to destroy dgram recv cq" );
        }
        free(qp);
        free(recv_cq);
        free(send_cq);
    }
}


/*
 * Destroy all resource
 */
void Verbs::finalize(){

    /* finalize qp control blocks */
    if( c_ctl ) c_ctl->finalize();
    if( ud_ctl ) ud_ctl->finalize();

    /* clean protection domain */
    if( protection_domain ){
        int retval = ibv_dealloc_pd( protection_domain );
        ASSERT( 0==retval, "Failed destroy protectiondomain ");
        protection_domain = NULL;
    }

    /* clean context */
    if( context ){
        int retval = ibv_close_device( context );
        ASSERT( 0==retval, "Failed to destroy context " );
        context = NULL;
    }
	/* clear device ptr */
    device = NULL;
}


/*
 * Create connected QPs and transit them to INIT
 */
void C_Ctl::_create_qps(){
	if( 0 == num_qps ) return;

	// malloc qps and cps
	qp = (struct ibv_qp **) malloc(num_qps * sizeof(struct ibv_qp *));
	cq = (struct ibv_cq **) malloc(num_qps * sizeof(struct ibv_cq *));
	ASSERT( qp!=NULL && cq!=NULL, "Failed to malloc memory for qp ans cq");

	for(int i = 0; i < num_qps; ++i){
		//create cq
		cq[i] = ibv_create_cq( context, DFT_Q_DEPTH, NULL, NULL, 0 );
		ASSERT( cq[i]!=NULL, "Failed to create cq");

#if USE_UC
		//create qp
		struct ibv_qp_init_attr init_attr;	
		memset(&init_attr, 0, sizeof(struct ibv_qp_init_attr));
		init_attr.send_cq = cq[i];
		init_attr.recv_cq = cq[i];
		init_attr.qp_type = IBV_QPT_UC;
		init_attr.cap.max_send_wr = DFT_Q_DEPTH;
		init_attr.cap.max_recv_wr = 1; // why?
		init_attr.cap.max_send_sge = 1;
		init_attr.cap.max_recv_sge = 1;
		init_attr.cap.max_inline_data = DFT_MAX_INLINE;

		qp[i] = ibv_create_qp(protection_domain, &init_attr);
		ASSERT( qp[i]!=NULL, "Faild to create conn qp");

		// move to INIT
		struct ibv_qp_attr attr;
		memset(&attr, 0, sizeof(struct ibv_qp_attr));
		attr.qp_state = IBV_QPS_INIT;
		attr.port_num = port;
		attr.pkey_index = 0;
		attr.qp_access_flags = ( IBV_ACCESS_LOCAL_WRITE
						    | IBV_ACCESS_REMOTE_WRITE);
		int retval = ibv_modify_qp( qp[i], &attr
				, IBV_QP_STATE
				| IBV_QP_PKEY_INDEX
				| IBV_QP_PORT
				| IBV_QP_ACCESS_FLAGS);
		ASSERT( 0==retval, "Failed to transit conn QP to INIT")
#endif

#if USE_RC
		//create qp
		struct ibv_qp_init_attr init_attr;	
		memset(&init_attr, 0, sizeof(struct ibv_qp_init_attr));
		//init_attr.pd = protection_domain;
		init_attr.send_cq = cq[i];
		init_attr.recv_cq = cq[i];
		init_attr.qp_type = IBV_QPT_RC;
		init_attr.cap.max_send_wr = DFT_Q_DEPTH;
		init_attr.cap.max_recv_wr = 1; // why?
		init_attr.cap.max_send_sge = 1;
		init_attr.cap.max_recv_sge = 1;
		init_attr.cap.max_inline_data = DFT_MAX_INLINE;

		qp[i] = ibv_create_qp(protection_domain, &init_attr);
		ASSERT( qp[i]!=NULL, "Faild to create qp");

		// move to INIT
		struct ibv_qp_attr attr;
		memset(&attr, 0, sizeof(struct ibv_qp_attr));
		attr.qp_state = IBV_QPS_INIT;
		attr.port_num = port;
		attr.pkey_index = 0;
		attr.qp_access_flags = ( IBV_ACCESS_LOCAL_WRITE
						    | IBV_ACCESS_REMOTE_WRITE
							| IBV_ACCESS_REMOTE_READ
							| IBV_ACCESS_REMOTE_ATOMIC );
		int retval = ibv_modify_qp( qp[i], &attr
				, IBV_QP_STATE
				| IBV_QP_PKEY_INDEX
				| IBV_QP_PORT
				| IBV_QP_ACCESS_FLAGS);
		ASSERT( 0==retval, "Failed to transit conn QP to INIT")
#endif
	}
}

void C_Ctl::__connect_qps(int i, struct remote_qp_attr_t *remote_qp_attr ){
	ASSERT( i>=0 && i<num_qps, "invalid arg i");
	ASSERT( qp[i] != NULL, "target qp is invalid" );

#if USE_UC
	struct ibv_qp_attr conn_attr;
	// move to RTR
	memset(&conn_attr, 0, sizeof(struct ibv_qp_attr));
	conn_attr.qp_state = IBV_QPS_RTR;
	//IBV_MTU_256 IBV_MTU_512 IBV_MTU_1024 IBV_MTU_2048
	conn_attr.path_mtu = IBV_MTU_4096;
	conn_attr.dest_qp_num = remote_qp_attr->qpn;
	conn_attr.rq_psn = DFT_PSN;

	conn_attr.ah_attr.is_global = 0;
	conn_attr.ah_attr.dlid = remote_qp_attr->lid;
	conn_attr.ah_attr.sl = 0;
	conn_attr.ah_attr.src_path_bits = 0;
	conn_attr.ah_attr.port_num = port;
	
	int retval = ibv_modify_qp( qp[i], &conn_attr
			 , IBV_QP_STATE
			 | IBV_QP_PATH_MTU
			 | IBV_QP_DEST_QPN
			 | IBV_QP_RQ_PSN
			 | IBV_QP_AV );
	ASSERT(0==retval, "Failed to transit QP to RTR");

	// move to RTS
	memset(&conn_attr, 0, sizeof(struct ibv_qp_attr));
	conn_attr.qp_state = IBV_QPS_RTS;
	conn_attr.sq_psn = DFT_PSN;

	retval = ibv_modify_qp( qp[i], &conn_attr
			, IBV_QP_STATE
			| IBV_QP_SQ_PSN );
	ASSERT(0==retval, "Failed to transit QP to RTS");
#endif
#if USE_RC
	struct ibv_qp_attr conn_attr;
	// move to RTR
	memset(&conn_attr, 0, sizeof(struct ibv_qp_attr));
	conn_attr.qp_state = IBV_QPS_RTR;
	//IBV_MTU_256 IBV_MTU_512 IBV_MTU_1024 IBV_MTU_2048
	conn_attr.path_mtu = IBV_MTU_4096;
	conn_attr.dest_qp_num = remote_qp_attr->qpn;
	conn_attr.rq_psn = DFT_PSN;

	conn_attr.ah_attr.is_global = 0;
	conn_attr.ah_attr.dlid = remote_qp_attr->lid;
	conn_attr.ah_attr.sl = 0;
	conn_attr.ah_attr.src_path_bits = 0;
	conn_attr.ah_attr.port_num = port;

	conn_attr.max_dest_rd_atomic = DFT_MAX_DEST_RD_ATOMIC;
	conn_attr.min_rnr_timer = DFT_MIN_RNR_TIMER;
	
	int retval = ibv_modify_qp( qp[i], &conn_attr
			 , IBV_QP_STATE
			 | IBV_QP_PATH_MTU
			 | IBV_QP_DEST_QPN
			 | IBV_QP_RQ_PSN
			 | IBV_QP_AV 
			 | IBV_QP_MAX_DEST_RD_ATOMIC
			 | IBV_QP_MIN_RNR_TIMER );
	ASSERT(0==retval, "Failed to transit QP to RTR");

	// move to RTS
	memset(&conn_attr, 0, sizeof(struct ibv_qp_attr));
	conn_attr.qp_state = IBV_QPS_RTS;
	conn_attr.sq_psn = DFT_PSN;
	conn_attr.timeout = DFT_TIMEOUT;
	conn_attr.retry_cnt = DFT_RETRY_CNT;
	conn_attr.rnr_retry = DFT_RNR_RETRY;
	conn_attr.max_rd_atomic = DFT_MAX_RD_ATOMIC;

	retval = ibv_modify_qp( qp[i], &conn_attr
			, IBV_QP_STATE
			| IBV_QP_SQ_PSN 
			| IBV_QP_TIMEOUT
			| IBV_QP_RETRY_CNT
			| IBV_QP_RNR_RETRY
			| IBV_QP_MAX_QP_RD_ATOMIC);
	ASSERT(0==retval, "Failed to transit QP to RTS");
#endif
}


void C_Ctl::_connect_qps(){
	for(int i = 0; i < mpi_env->size; ++i){
		if(i != mpi_env->rank) 
			__connect_qps(i, &remote_qps[i]);
	}
}

void UD_Ctl::_build_ah(){
	int is_inited = 0;
	MPI_Initialized( &is_inited );
	ASSERT( is_inited, "MPI env should be initialized first" );

	int _size = mpi_env->size;
	int _rank = mpi_env->rank;
	ah = (struct ibv_ah **) malloc( _size * sizeof(struct ibv_ah *) );

    /* ah[i] stand for address handler for rank i */
	struct ibv_ah_attr ah_attr;
	for(int i = 0;  i < _size; ++i){
		memset(&ah_attr, 0, sizeof(struct ibv_ah_attr));
		if( i == _rank ) continue;
		ah_attr.is_global = 0;
		ah_attr.dlid = remote_qps[i].lid;
        //printf("%d lid: %d\n",i, ah_attr.dlid);
		ah_attr.sl = 0;
		ah_attr.src_path_bits = 0;
		ah_attr.port_num = port;
		ah[i] = ibv_create_ah(protection_domain, &ah_attr);
		ASSERT( ah[i]!= NULL, "Failed to create ah for dgram qps");
	}
}

/*
 * Create datagram QPs and transit them to RTS
 */
void UD_Ctl::_create_qps(){
	if( 0 == num_qps ) return;

	/* malloc qps and cps */
	qp = (struct ibv_qp **) malloc( num_qps * sizeof(struct ibv_qp *) );
	send_cq = (struct ibv_cq **) malloc( num_qps * sizeof(struct ibv_cq *) );
	recv_cq = (struct ibv_cq **) malloc( num_qps * sizeof(struct ibv_cq *) );
	ASSERT( qp!=NULL && send_cq!=NULL && recv_cq!=NULL, "Failed to malloc memory for qp ans cq" );

	for(int i = 0; i < num_qps; ++i){
		/* create cp */
		send_cq[i] = ibv_create_cq( context, DFT_Q_DEPTH, NULL, NULL, 0 );
		ASSERT( send_cq[i]!=NULL, "Failed to create dgram send cq" );
		recv_cq[i] = ibv_create_cq( context, DFT_Q_DEPTH, NULL, NULL, 0 );
		ASSERT( recv_cq[i]!=NULL, "Failed to create dgram recv cq" );

		/* create pq*/
		struct ibv_qp_init_attr init_attr;
		memset( (void *) &init_attr, 0, sizeof(struct ibv_qp_init_attr) );
		init_attr.send_cq = send_cq[i];
		init_attr.recv_cq = recv_cq[i];
		init_attr.qp_type = IBV_QPT_UD;

		init_attr.cap.max_send_wr = DFT_Q_DEPTH;
		init_attr.cap.max_recv_wr = DFT_Q_DEPTH;
		init_attr.cap.max_send_sge = 1;
		init_attr.cap.max_recv_sge = 1;
		init_attr.cap.max_inline_data = DFT_MAX_INLINE;

		qp[i] = ibv_create_qp( protection_domain, &init_attr );
		ASSERT( qp[i]!=NULL, "Failed to create dgram qp" );

		/* move to INIT */
		struct ibv_qp_attr attr;
		memset( &attr, 0, sizeof(struct ibv_qp_attr) );
		attr.qp_state = IBV_QPS_INIT;
		attr.pkey_index = 0;
		attr.port_num = port;
		attr.qkey = DFT_QKEY;
		//attr.qp_access_flags = ( IBV_ACCESS_LOCAL_WRITE );

		int retval = ibv_modify_qp( qp[i], &attr
				, IBV_QP_STATE
				| IBV_QP_PKEY_INDEX
				| IBV_QP_PORT
				| IBV_QP_QKEY);
		ASSERT( 0==retval, "Failed to transit qp to INIT" );

		/* move to RTR */
		memset( &attr, 0, sizeof(struct ibv_qp_attr) );
		attr.qp_state = IBV_QPS_RTR;
		//attr.path_mtu = IBV_MTU_4096;

		retval = ibv_modify_qp( qp[i], &attr
				, IBV_QP_STATE );
                //| IBV_QP_PATH_MTU);
		ASSERT( 0==retval, "Failed to transit qp to RTR" );

		/* move to RTS */
		memset( &attr, 0, sizeof(struct ibv_qp_attr) );
		attr.qp_state = IBV_QPS_RTS;
		attr.sq_psn = DFT_PSN;
		
		retval = ibv_modify_qp( qp[i], &attr
				, IBV_QP_STATE //);
				| IBV_QP_SQ_PSN);
		ASSERT( 0==retval, "Failed to transit qp to RTS" );
	}
}

/*
 * alloc rdma buffer for every node
 * take no account for connect SEND/RECV
 */
void C_Ctl::_alloc_qp_buf() {
	recv_buf = (uint8_t *) malloc( recv_buf_size * sizeof(uint8_t) );
	recv_buf_mr = ibv_reg_mr( protection_domain, recv_buf, recv_buf_size
			, IBV_ACCESS_LOCAL_WRITE
			| IBV_ACCESS_REMOTE_READ
			| IBV_ACCESS_REMOTE_WRITE
			| IBV_ACCESS_REMOTE_ATOMIC );

	send_buf = (uint8_t *) malloc( send_buf_size * sizeof(uint8_t) );
	send_buf_mr = ibv_reg_mr( protection_domain, send_buf, send_buf_size
			, IBV_ACCESS_LOCAL_WRITE );
}

/*
 * alloc dgram buffer for every node
 */
void UD_Ctl::_alloc_qp_buf() {
	recv_buf = (uint8_t *) malloc( recv_buf_size * sizeof(uint8_t) );
	recv_buf_mr = ibv_reg_mr( protection_domain, recv_buf, recv_buf_size
			, IBV_ACCESS_LOCAL_WRITE //);
			| IBV_ACCESS_REMOTE_READ 
			| IBV_ACCESS_REMOTE_WRITE 
            | IBV_ACCESS_REMOTE_ATOMIC );
			
	send_buf = (uint8_t *) malloc( send_buf_size * sizeof(uint8_t) );
	send_buf_mr = ibv_reg_mr( protection_domain, send_buf, send_buf_size
			, IBV_ACCESS_LOCAL_WRITE );
}

/* 
 * The following information needs to be exchanged before connect QPs
 *  - remote_rdma_buff ( UC && RC )
 *  - remote_rdma_buff_size ( UC && RC )
 *  - remote_rdma_buff_rkey ( UC && RC )
 *  - port lid
 *  - qpn
 *  - psn ( use default; UC && RC )
 *  - qkey ( use default; UD only )
 */
void C_Ctl::_exchange_remote_qp_info(){
	int is_inited = 0;
	MPI_Initialized( &is_inited );
	ASSERT( is_inited, "MPI env should be initialized first" );

	/* how mant remote qp info should I keep */
	int _size = mpi_env->size;
	remote_qps = (struct remote_qp_attr_t *) malloc( _size * sizeof(struct remote_qp_attr_t) );

    mpi_env->barrier();
	/* exchange lid */
	uint16_t *lids = (uint16_t *) malloc( _size * sizeof(uint16_t) );
	memset( lids, 0, sizeof(lids) );
	MPI_Allgather( &port_attr.lid, 1, MPI_UINT16_T, &lids[0], 1, MPI_UINT16_T, mpi_env->comm );
	for(int i = 0; i < _size; ++i)remote_qps[i].lid = lids[i];
	free(lids);

	/* exchange qpn */
	uint32_t *qpns = (uint32_t *) malloc( _size * sizeof(uint32_t) );
	uint32_t *my_qpns = (uint32_t *) malloc( _size * sizeof(uint32_t) );
	memset( qpns, 0, sizeof(qpns) );
	for(int i = 0; i < _size; ++i) my_qpns[i] = qp[i]->qp_num;
	MPI_Alltoall( &my_qpns[0], 1, MPI_UINT32_T, &qpns[0], 1, MPI_UINT32_T, mpi_env->comm );
	for(int i = 0; i < _size; ++i) remote_qps[i].qpn = qpns[i];
	free(qpns);
	free(my_qpns);

	/* exchange rdma buffer info (conn_qp only) */
	uint64_t *rdma_buf = (uint64_t *) malloc( _size * sizeof(uint64_t) );
	uint32_t *rdma_buf_size = (uint32_t *) malloc( _size * sizeof(uint32_t) );
	uint32_t *rkey = (uint32_t *) malloc( _size * sizeof(uint32_t) );
	memset( rdma_buf, 0, sizeof(rdma_buf) );
	memset( rdma_buf_size, 0, sizeof(rdma_buf_size) );
	memset( rkey, 0, sizeof(rkey) );
	uint64_t easy_for_debug = reinterpret_cast<uint64_t>( recv_buf );
	MPI_Allgather( &easy_for_debug, 1, MPI_UINT64_T, &rdma_buf[0], 1, MPI_UINT64_T, mpi_env->comm );
	MPI_Allgather( &recv_buf_size, 1, MPI_INT, &rdma_buf_size[0], 1, MPI_INT, mpi_env->comm );
	MPI_Allgather( &recv_buf_mr->rkey, 1, MPI_UINT32_T, &rkey[0], 1, MPI_UINT32_T, mpi_env->comm );
	for(int i = 0 ; i < _size; ++i){
		remote_qps[i].rdma_buf = reinterpret_cast<uintptr_t>(rdma_buf[i]);
		remote_qps[i].rdma_buf_size = rdma_buf_size[i];
		remote_qps[i].rkey = rkey[i];
	}
	free(rdma_buf);
	free(rdma_buf_size);
	free(rkey);
    mpi_env->barrier();
}

/* ditto */
void UD_Ctl::_exchange_remote_qp_info(){
	int is_inited = 0;
	MPI_Initialized( &is_inited );
	ASSERT( is_inited, "MPI env should be initialized first" );

	/* how mant remote qp info should I keep */
	int _size = mpi_env->size;
	remote_qps = (struct remote_qp_attr_t *) malloc( _size * sizeof(struct remote_qp_attr_t) );

    mpi_env->barrier();
	/* exchange lid */
    uint16_t *lids = (uint16_t *) malloc( _size * sizeof(uint16_t) );
	memset( lids, 0, sizeof(lids) );
	MPI_Allgather( &port_attr.lid, 1, MPI_UINT16_T, &lids[0], 1, MPI_UINT16_T, mpi_env->comm );
	for(int i = 0; i < _size; ++i) remote_qps[i].lid = lids[i];
	free(lids);

	/* exchange qpn */
    uint32_t *qpns = (uint32_t *) malloc( _size * sizeof(uint32_t) );
    memset( qpns, 0, sizeof(qpns) );
	MPI_Allgather( &qp[0]->qp_num, 1, MPI_UINT32_T, &qpns[0], 1, MPI_UINT32_T, mpi_env->comm );
	for(int i = 0; i < _size; ++i) remote_qps[i].qpn = qpns[i];
    free(qpns);

    mpi_env->barrier();
}

/* build MPI env */
void mpi_env_t::mpi_env_init(int *argc, char** argv[]){
    int is_inited = 0;
    MPI_Initialized( &is_inited );
    if(is_inited) return;
    MPI_Init(argc, argv);
    comm = MPI_COMM_WORLD;
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );
    MPI_Comm_size( MPI_COMM_WORLD, &size );

    // check that every node should have only one MPI process.
    MPI_Comm _tmp_comm;
	int num = 0;
    MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &_tmp_comm);
    MPI_Comm_size(_tmp_comm, &num);
    ASSERT( num==1, "Every node should have only one MPI process" );
}

void mpi_env_t::mpi_env_finalize(){
    int is_finalized = 0;
    MPI_Finalized( &is_finalized );
    if( is_finalized ) return;

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}

void mpi_env_t::barrier(){
	MPI_Barrier(comm);
}

void C_Ctl::post_RDMA_write(int rank, uint8_t* msg, int size){
	struct ibv_send_wr wr, *bad_send_wr;
	struct ibv_sge sgl;

	memset(&sgl, 0, sizeof(sgl));
	memset(&wr, 0, sizeof(wr));

	auto send_buf_addr = this->send_buf + offset;
	memcpy(send_buf_addr, msg, size);
	offset += size;

	wr.opcode = IBV_WR_RDMA_WRITE;
	wr.num_sge = 1;
	wr.next = NULL;
	wr.sg_list = &sgl;
	wr.send_flags = IBV_SEND_SIGNALED;

	sgl.addr = reinterpret_cast<uint64_t>(send_buf_addr);
	sgl.length = size;
	sgl.lkey = send_buf_mr->lkey;

	wr.wr.rdma.remote_addr = remote_qps[rank].rdma_buf;
	wr.wr.rdma.rkey = remote_qps[rank].rkey;

	int retval = ibv_post_send( qp[rank], &wr, &bad_send_wr );
	ASSERT( 0==retval, "Failed to post a RDMA WRITE wr" );
}


void UD_Ctl::post_UD_send(int rank, uint8_t* msg, int size){
	struct ibv_send_wr wr, *bad_send_wr;
	struct ibv_sge sgl;

	memset(&sgl, 0, sizeof(sgl));
	memset(&wr, 0, sizeof(wr));

	uint8_t* send_buf_addr = this->send_buf + offset;
	memcpy(send_buf_addr, msg, size);
	offset += size;

	wr.wr.ud.ah = ah[rank];
	wr.wr.ud.remote_qpn = remote_qps[rank].qpn;
	//printf("qpn: %d\n", remote_qps[rank].qpn);
	wr.wr.ud.remote_qkey = DFT_QKEY;
	wr.opcode = IBV_WR_SEND;
	wr.num_sge = 1;
	wr.next = NULL;
	wr.sg_list = &sgl;
	wr.send_flags = IBV_SEND_SIGNALED;
	sgl.addr = reinterpret_cast<uint64_t>(send_buf_addr);
	sgl.length = size;
	sgl.lkey = send_buf_mr->lkey;

	int retval = ibv_post_send( qp[0], &wr, &bad_send_wr );
	ASSERT( 0==retval, "Failed to post a UD SEND wr" );
}

void UD_Ctl::post_UD_recv(int size){
	struct ibv_recv_wr wr, *bad_recv_wr;
	struct ibv_sge sgl;

	uint8_t* recv_buf_addr = this->recv_buf + tail;
	tail += size;

	memset(&sgl, 0, sizeof(sgl));
	memset(&wr, 0, sizeof(wr));

	//printf("qpn: %d\n", qp[0]->qp_num);
	//printf("lid: %d\n", port_attr.lid);
	sgl.addr = reinterpret_cast<uint64_t>(recv_buf_addr);
	sgl.length = size;
	sgl.lkey = recv_buf_mr->lkey;

	wr.sg_list = &sgl;
	wr.num_sge = 1;
	wr.next = NULL;

	int retval = ibv_post_recv( qp[0], &wr, &bad_recv_wr );
	ASSERT( 0==retval, "Failed to post UD RECV wr");
}

void UD_Ctl::poll_cq(int num, int send){
	int comps = 0;
	struct ibv_wc wc;
	struct ibv_cq *t_cq = ( send == 1? send_cq[0]: recv_cq[0] );
	while(comps < num){
		int new_comps = ibv_poll_cq(t_cq, 1, &wc);
		if( new_comps !=0 ){
			ASSERT( new_comps>0, "Fatal error, poll failed");
			//printf("%s\n", ibv_wc_status_str(wc.status));
			ASSERT( wc.status==IBV_WC_SUCCESS, "Fatal error, work not complete" );
			//printf("%d Yes\n", mpi_env->rank);
			comps ++;
		}
	}
}

void C_Ctl::poll_cq(int num, int rank){
	int comps = 0;
	struct ibv_wc wc;
	struct ibv_cq *t_cq = cq[rank];
	while(comps < num){
		int new_comps = ibv_poll_cq(t_cq, 1, &wc);
		if( new_comps !=0 ){
			ASSERT( new_comps>0, "Fatal error, poll failed");
			//printf("%s\n", ibv_wc_status_str(wc.status));
			ASSERT( wc.status==IBV_WC_SUCCESS, "Fatal error, work not complete" );
			//printf("%d Yes", mpi_env.rank);
			comps ++;
		}
	}
}

void C_Ctl::init( int num_conn_qps, int send_buf_size, int recv_buf_size ){
    num_qps = num_conn_qps;
    this->send_buf_size = send_buf_size;
    this->recv_buf_size = recv_buf_size;
    offset = head = tail = dispatch = 0;
    _create_qps();
    _alloc_qp_buf();
    _exchange_remote_qp_info();
    _connect_qps();
    mpi_env->barrier();
}

void UD_Ctl::init( int num_dgram_qps, int send_buf_size, int recv_buf_size ){
    num_qps = num_dgram_qps;
    this->send_buf_size = send_buf_size;
    this->recv_buf_size = recv_buf_size;
    offset = head = tail = dispatch = 0;
    _create_qps();
    _alloc_qp_buf();
    _exchange_remote_qp_info();
    _build_ah();
    mpi_env->barrier();
}

void Verbs::post_send(int use_ud, int rank, uint8_t* msg, int size){
    if(use_ud){
        ud_ctl->post_UD_send(rank, msg, size);
    }else{
        c_ctl->post_RDMA_write(rank, msg, size);
    }
}

void Verbs::post_recv(int use_ud, int size){
    if(use_ud){
        ud_ctl->post_UD_recv(size);
    }else{
        ASSERT( false, "not implemented for UC&&RC send/recv" );
    }
}

void Verbs::poll_cq(int use_ud, int num, int x){
    if(use_ud){
        ud_ctl->poll_cq(num, x);
    }else{
        c_ctl->poll_cq(num, x);
    }
}

void Verbs::DEBUG(int use_ud){
    if(use_ud){
        ud_ctl->DEBUG();
    }else{
        c_ctl->DEBUG();
    }
}

void C_Ctl::DEBUG(){
    for(int i = 0 ; i < 10 ; ++i){
        std::cout << recv_buf[i] << " ";
    }
    std::cout << std::endl;
}

void UD_Ctl::DEBUG(){
    for(int i = 40 ; i < 50 ; ++i){
        std::cout << recv_buf[i] << " ";
    }
    std::cout << std::endl;
}


Verbs* global_verbs;
mpi_env_t* mpi_env;

void RDMA_Init(int* argc, char** argv[]){
    mpi_env = new mpi_env_t();
    mpi_env->mpi_env_init(argc, argv);
    //global_verbs = new Verbs(2, 0, 8192, 8192, 0, 0);
    global_verbs = new Verbs(0, 1, 0, 0, 8192, 8192);
}

void RDMA_Finalize(){
    global_verbs->finalize();
    mpi_env->mpi_env_finalize();
}

Verbs* get_verb(){
	return global_verbs;
}

mpi_env_t * get_mpi_env(){
    return mpi_env;
}
