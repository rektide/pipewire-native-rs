// SPDX-License-Identifier: MIT
// SPDX-FileCopyrightText: Copyright (c) 2026 Asymptotic Inc.

use pipewire_native_macros::EnumU32;

/// Backing-memory kinds from upstream `enum spa_data_type`.
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumU32)]
pub enum DataType {
    Invalid,
    MemPtr,
    MemFd,
    DmaBuf,
    MemId,
    SyncObj,
}

/// `SPA_DATA_Invalid`.
pub const INVALID: u32 = DataType::Invalid as u32;
/// `SPA_DATA_MemPtr`.
pub const MEM_PTR: u32 = DataType::MemPtr as u32;
/// `SPA_DATA_MemFd`.
pub const MEM_FD: u32 = DataType::MemFd as u32;
/// `SPA_DATA_DmaBuf`.
pub const DMA_BUF: u32 = DataType::DmaBuf as u32;
/// `SPA_DATA_MemId`.
pub const MEM_ID: u32 = DataType::MemId as u32;
/// `SPA_DATA_SyncObj`.
pub const SYNC_OBJ: u32 = DataType::SyncObj as u32;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn data_types_match_upstream_abi() {
        assert_eq!(INVALID, 0);
        assert_eq!(MEM_PTR, 1);
        assert_eq!(MEM_FD, 2);
        assert_eq!(DMA_BUF, 3);
        assert_eq!(MEM_ID, 4);
        assert_eq!(SYNC_OBJ, 5);
    }

    #[test]
    fn data_type_converts_to_and_from_wire_values() {
        assert_eq!(u32::from(DataType::MemFd), MEM_FD);
        assert_eq!(DataType::try_from(DMA_BUF), Ok(DataType::DmaBuf));
        assert_eq!(DataType::try_from(SYNC_OBJ + 1), Err(()));
    }
}
