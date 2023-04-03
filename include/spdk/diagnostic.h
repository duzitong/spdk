#ifndef SPDK_DIAGNOSTIC_H_
#define SPDK_DIAGNOSTIC_H_

#include "spdk/stdinc.h"
#include "spdk/log.h"

#ifdef __cplusplus
extern "C" {
#endif

struct diagnostic_read_md {
    int target_id;
    bool force_read_from_disk;
};

#ifdef __cplusplus
}
#endif

#endif /* SPDK_RDMA_CONNECTION_H_ */