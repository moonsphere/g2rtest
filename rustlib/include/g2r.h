#ifndef G2R_H
#define G2R_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uintptr_t buffer_ptr;
    uintptr_t buffer_len;
    uintptr_t head_ptr;
    uintptr_t tail_ptr;
    uintptr_t working_ptr;
    uintptr_t stuck_ptr;
    int32_t working_fd;
    int32_t unstuck_fd;
} G2RQueueMeta;

typedef struct {
    G2RQueueMeta request_queue;
    G2RQueueMeta response_queue;
    uint32_t slot_capacity;
} G2RInitResult;

int32_t g2r_init(uint32_t queue_capacity, G2RInitResult *out_result);

#ifdef __cplusplus
}
#endif

#endif // G2R_H
