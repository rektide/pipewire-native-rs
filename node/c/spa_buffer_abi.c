// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

#include <stddef.h>
#include <stdio.h>

#include <spa/buffer/buffer.h>
#include <spa/node/io.h>

int main(void)
{
#define PRINT_VALUE(name, expression) \
    printf("pub const %s: usize = %zu;\n", #name, (size_t)(expression))
    PRINT_VALUE(IO_BUFFERS_SIZE, sizeof(struct spa_io_buffers));
    PRINT_VALUE(IO_BUFFERS_ALIGN, _Alignof(struct spa_io_buffers));
    PRINT_VALUE(IO_STATUS, offsetof(struct spa_io_buffers, status));
    PRINT_VALUE(IO_BUFFER_ID, offsetof(struct spa_io_buffers, buffer_id));
    PRINT_VALUE(CHUNK_SIZE, sizeof(struct spa_chunk));
    PRINT_VALUE(CHUNK_ALIGN, _Alignof(struct spa_chunk));
    PRINT_VALUE(CHUNK_OFFSET, offsetof(struct spa_chunk, offset));
    PRINT_VALUE(CHUNK_DATA_SIZE, offsetof(struct spa_chunk, size));
    PRINT_VALUE(CHUNK_STRIDE, offsetof(struct spa_chunk, stride));
    PRINT_VALUE(CHUNK_FLAGS, offsetof(struct spa_chunk, flags));
    return 0;
}
