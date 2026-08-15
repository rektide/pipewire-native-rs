// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

#include <stddef.h>
#include <stdio.h>

#include "pipewire/private.h"

#define ABI_VALUE(name, expression) const size_t pw_activation_abi_##name = (expression)

#ifdef PW_ACTIVATION_ABI_PROBE
int main(void)
{
#define PRINT_VALUE(name, expression) \
    printf("pub const %s: usize = %zu;\n", #name, (size_t)(expression))
    PRINT_VALUE(SIZE, sizeof(struct pw_node_activation));
    PRINT_VALUE(ALIGN, _Alignof(struct pw_node_activation));
    PRINT_VALUE(STATUS, offsetof(struct pw_node_activation, status));
    PRINT_VALUE(STATE0_STATUS, offsetof(struct pw_node_activation, state[0].status));
    PRINT_VALUE(STATE0_REQUIRED, offsetof(struct pw_node_activation, state[0].required));
    PRINT_VALUE(STATE0_PENDING, offsetof(struct pw_node_activation, state[0].pending));
    PRINT_VALUE(SIGNAL_TIME, offsetof(struct pw_node_activation, signal_time));
    PRINT_VALUE(AWAKE_TIME, offsetof(struct pw_node_activation, awake_time));
    PRINT_VALUE(FINISH_TIME, offsetof(struct pw_node_activation, finish_time));
    PRINT_VALUE(CLIENT_VERSION, offsetof(struct pw_node_activation, client_version));
    PRINT_VALUE(SERVER_VERSION, offsetof(struct pw_node_activation, server_version));
    return 0;
}
#else
ABI_VALUE(size, sizeof(struct pw_node_activation));
ABI_VALUE(align, _Alignof(struct pw_node_activation));
ABI_VALUE(status, offsetof(struct pw_node_activation, status));
ABI_VALUE(state0_status, offsetof(struct pw_node_activation, state[0].status));
ABI_VALUE(state0_required, offsetof(struct pw_node_activation, state[0].required));
ABI_VALUE(state0_pending, offsetof(struct pw_node_activation, state[0].pending));
ABI_VALUE(signal_time, offsetof(struct pw_node_activation, signal_time));
ABI_VALUE(awake_time, offsetof(struct pw_node_activation, awake_time));
ABI_VALUE(finish_time, offsetof(struct pw_node_activation, finish_time));
ABI_VALUE(client_version, offsetof(struct pw_node_activation, client_version));
ABI_VALUE(server_version, offsetof(struct pw_node_activation, server_version));
#endif
