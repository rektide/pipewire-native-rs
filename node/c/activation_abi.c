// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

#include <stddef.h>
#include <stdio.h>

#include "pipewire/private.h"

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
