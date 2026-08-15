use pipewire_native_protocol::native::frame::{
    FlushOutcome, FrameError, FrameLimits, FrameReceiver, FrameSender, Header, OutboundFrame,
    ReceiveOutcome, HEADER_LEN, WIRE_MAX_PAYLOAD,
};
use std::io::{Read, Write};
use std::os::fd::{AsFd, AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

fn wire(object_id: u32, opcode: u8, seq: u32, payload: &[u8], n_fds: u32) -> Vec<u8> {
    let header = Header {
        object_id,
        opcode,
        payload_len: payload.len() as u32,
        seq,
        n_fds,
    };
    let mut bytes = header.encode().unwrap().to_vec();
    bytes.extend_from_slice(payload);
    bytes
}

fn receive_frame(
    receiver: &mut FrameReceiver,
    socket: &UnixStream,
) -> pipewire_native_protocol::native::frame::ReceivedFrame {
    loop {
        match receiver.receive(socket.as_fd()).unwrap() {
            ReceiveOutcome::Frame(frame) => return frame,
            ReceiveOutcome::WouldBlock => std::thread::yield_now(),
            ReceiveOutcome::Closed => panic!("closed before frame"),
        }
    }
}

fn pipe() -> (OwnedFd, OwnedFd) {
    let mut fds = [0; 2];
    assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
    unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) }
}

fn send_with_fd(socket: &UnixStream, bytes: &[u8], fd: &OwnedFd) {
    let mut iov = libc::iovec {
        iov_base: bytes.as_ptr().cast_mut().cast(),
        iov_len: bytes.len(),
    };
    let mut control = [0_usize; 4];
    let header_len = std::mem::size_of::<libc::cmsghdr>();
    unsafe {
        std::ptr::write(
            control.as_mut_ptr().cast::<libc::cmsghdr>(),
            libc::cmsghdr {
                cmsg_len: header_len + std::mem::size_of::<libc::c_int>(),
                cmsg_level: libc::SOL_SOCKET,
                cmsg_type: libc::SCM_RIGHTS,
            },
        );
        std::ptr::write_unaligned(
            control
                .as_mut_ptr()
                .cast::<u8>()
                .add(header_len)
                .cast::<libc::c_int>(),
            fd.as_raw_fd(),
        );
    }
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = control.as_mut_ptr().cast();
    msg.msg_controllen =
        (header_len + std::mem::size_of::<libc::c_int>() + std::mem::size_of::<usize>() - 1)
            & !(std::mem::size_of::<usize>() - 1);
    assert_eq!(
        unsafe { libc::sendmsg(socket.as_raw_fd(), &msg, libc::MSG_NOSIGNAL) },
        bytes.len() as isize
    );
}

#[test]
fn header_known_vector_and_wire_max_round_trip() {
    let header = Header {
        object_id: 0x0102_0304,
        opcode: 0xa5,
        payload_len: 0x00b6_c7d8,
        seq: 0x1122_3344,
        n_fds: 0x5566_7788,
    };
    let expected_words = [0x0102_0304_u32, 0xa5b6_c7d8, 0x1122_3344, 0x5566_7788];
    let expected: Vec<_> = expected_words
        .into_iter()
        .flat_map(u32::to_ne_bytes)
        .collect();
    assert_eq!(header.encode().unwrap().as_slice(), expected);
    assert_eq!(Header::decode(header.encode().unwrap()), header);

    let max = Header {
        payload_len: WIRE_MAX_PAYLOAD as u32,
        ..header
    };
    assert_eq!(Header::decode(max.encode().unwrap()), max);
    let too_large = Header {
        payload_len: WIRE_MAX_PAYLOAD as u32 + 1,
        ..header
    };
    assert!(matches!(
        too_large.encode(),
        Err(FrameError::PayloadTooLarge { .. })
    ));
}

#[test]
fn every_byte_split_produces_the_same_frame() {
    let bytes = wire(7, 3, 99, b"segmented payload", 0);
    for split in 1..bytes.len() {
        let (mut tx, rx) = UnixStream::pair().unwrap();
        let mut receiver = FrameReceiver::new(FrameLimits::default());
        tx.write_all(&bytes[..split]).unwrap();
        assert!(matches!(
            receiver.receive(rx.as_fd()).unwrap(),
            ReceiveOutcome::WouldBlock
        ));
        tx.write_all(&bytes[split..]).unwrap();
        let frame = receive_frame(&mut receiver, &rx);
        assert_eq!(frame.header().object_id, 7);
        assert_eq!(frame.header().opcode, 3);
        assert_eq!(frame.payload(), b"segmented payload");
    }
}

#[test]
fn one_byte_at_a_time_and_coalesced_frames_are_preserved() {
    let first = wire(1, 2, 3, b"first", 0);
    let second = wire(4, 5, 6, b"second", 0);
    let (mut tx, rx) = UnixStream::pair().unwrap();
    let mut receiver = FrameReceiver::new(FrameLimits::default());
    let mut delivered = None;
    for byte in &first {
        tx.write_all(&[*byte]).unwrap();
        let outcome = receiver.receive(rx.as_fd()).unwrap();
        match outcome {
            ReceiveOutcome::Frame(frame) => delivered = Some(frame),
            ReceiveOutcome::WouldBlock => {}
            ReceiveOutcome::Closed => panic!("closed during byte splits"),
        }
    }
    assert_eq!(delivered.unwrap().payload(), b"first");

    let (mut tx, rx) = UnixStream::pair().unwrap();
    let mut receiver = FrameReceiver::new(FrameLimits::default());
    tx.write_all(&[first, second].concat()).unwrap();
    let frame1 = receive_frame(&mut receiver, &rx);
    let frame2 = receive_frame(&mut receiver, &rx);
    assert_eq!(frame1.payload(), b"first");
    assert_eq!(frame2.payload(), b"second");
}

#[test]
fn eof_at_each_incomplete_position_is_truncated_and_poisoned() {
    let bytes = wire(1, 1, 1, b"payload", 0);
    for end in 1..bytes.len() {
        let (mut tx, rx) = UnixStream::pair().unwrap();
        tx.write_all(&bytes[..end]).unwrap();
        drop(tx);
        let mut receiver = FrameReceiver::new(FrameLimits::default());
        assert!(matches!(
            receiver.receive(rx.as_fd()),
            Err(FrameError::TruncatedFrame { .. })
        ));
        assert!(matches!(
            receiver.receive(rx.as_fd()),
            Err(FrameError::Poisoned)
        ));
        receiver.clear();
        assert!(matches!(
            receiver.receive(rx.as_fd()),
            Err(FrameError::Poisoned)
        ));
    }
}

#[test]
fn complete_frame_is_delivered_before_clean_eof() {
    let (mut tx, rx) = UnixStream::pair().unwrap();
    tx.write_all(&wire(1, 2, 3, b"last", 0)).unwrap();
    drop(tx);
    let mut receiver = FrameReceiver::new(FrameLimits::default());
    assert_eq!(receive_frame(&mut receiver, &rx).payload(), b"last");
    assert!(matches!(
        receiver.receive(rx.as_fd()).unwrap(),
        ReceiveOutcome::Closed
    ));
}

#[test]
fn fd_batches_partition_by_frame_and_take_is_indexed() {
    let (tx, rx) = UnixStream::pair().unwrap();
    let (read1, write1) = pipe();
    let (read2, write2) = pipe();
    let limits = FrameLimits::default();
    let mut sender = FrameSender::new(limits);
    sender
        .enqueue(OutboundFrame::new(1, 1, 1, b"a".to_vec(), vec![write1], limits).unwrap())
        .unwrap();
    sender
        .enqueue(OutboundFrame::new(2, 2, 2, b"b".to_vec(), vec![write2], limits).unwrap())
        .unwrap();
    assert_eq!(sender.flush(tx.as_fd()).unwrap(), FlushOutcome::Drained);

    let mut receiver = FrameReceiver::new(limits);
    let mut first = receive_frame(&mut receiver, &rx);
    let mut second = receive_frame(&mut receiver, &rx);
    assert_eq!(first.fds().len(), 1);
    assert_eq!(second.fds().len(), 1);
    assert!(first.fds().get(0).is_ok());
    let first_write = first.fds_mut().take(0).unwrap();
    assert!(matches!(
        first.fds_mut().take(0),
        Err(FrameError::FdAlreadyTaken { index: 0 })
    ));
    assert!(matches!(
        second.fds().get(1),
        Err(FrameError::InvalidFdIndex { .. })
    ));
    let second_write = second.fds_mut().take(0).unwrap();
    assert_eq!(
        unsafe { libc::write(first_write.as_raw_fd(), b"1".as_ptr().cast(), 1) },
        1
    );
    assert_eq!(
        unsafe { libc::write(second_write.as_raw_fd(), b"2".as_ptr().cast(), 1) },
        1
    );
    let mut byte = [0];
    assert_eq!(
        unsafe { libc::read(read1.as_raw_fd(), byte.as_mut_ptr().cast(), 1) },
        1
    );
    assert_eq!(byte, *b"1");
    assert_eq!(
        unsafe { libc::read(read2.as_raw_fd(), byte.as_mut_ptr().cast(), 1) },
        1
    );
    assert_eq!(byte, *b"2");
}

#[test]
fn dropping_unknown_frame_closes_its_received_fd() {
    let (tx, rx) = UnixStream::pair().unwrap();
    let (read, write) = pipe();
    let limits = FrameLimits::default();
    let mut sender = FrameSender::new(limits);
    sender
        .enqueue(OutboundFrame::new(999, 1, 1, vec![], vec![write], limits).unwrap())
        .unwrap();
    sender.flush(tx.as_fd()).unwrap();
    let mut receiver = FrameReceiver::new(limits);
    drop(receive_frame(&mut receiver, &rx));
    let flags = unsafe { libc::fcntl(read.as_raw_fd(), libc::F_GETFL) };
    unsafe { libc::fcntl(read.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
    let mut byte = [0];
    assert_eq!(
        unsafe { libc::read(read.as_raw_fd(), byte.as_mut_ptr().cast(), 1) },
        0
    );
}

#[test]
fn missing_fds_and_limits_are_terminal() {
    let (mut tx, rx) = UnixStream::pair().unwrap();
    tx.write_all(&wire(1, 1, 1, b"x", 1)).unwrap();
    let mut receiver = FrameReceiver::new(FrameLimits::default());
    assert!(matches!(
        receiver.receive(rx.as_fd()),
        Err(FrameError::MissingFds {
            declared: 1,
            available: 0
        })
    ));

    let (mut tx, rx) = UnixStream::pair().unwrap();
    tx.write_all(&wire(1, 1, 1, b"too long", 0)).unwrap();
    let mut receiver = FrameReceiver::new(FrameLimits {
        max_payload: 2,
        ..FrameLimits::default()
    });
    assert!(matches!(
        receiver.receive(rx.as_fd()),
        Err(FrameError::PayloadTooLarge { .. })
    ));
}

#[test]
fn extra_fds_at_eof_and_truncated_control_are_terminal_and_cleaned() {
    let limits = FrameLimits::default();
    let (tx, rx) = UnixStream::pair().unwrap();
    let (read, write) = pipe();
    send_with_fd(&tx, &wire(1, 1, 1, b"", 0), &write);
    drop(write);
    drop(tx);
    let mut receiver = FrameReceiver::new(limits);
    drop(receive_frame(&mut receiver, &rx));
    assert!(matches!(
        receiver.receive(rx.as_fd()),
        Err(FrameError::UnexpectedFdsAtEof { count: 1 })
    ));
    let flags = unsafe { libc::fcntl(read.as_raw_fd(), libc::F_GETFL) };
    unsafe { libc::fcntl(read.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
    let mut byte = [0];
    assert_eq!(
        unsafe { libc::read(read.as_raw_fd(), byte.as_mut_ptr().cast(), 1) },
        0
    );

    let (tx, rx) = UnixStream::pair().unwrap();
    let (read, write) = pipe();
    let mut sender = FrameSender::new(limits);
    sender
        .enqueue(OutboundFrame::new(1, 1, 1, vec![], vec![write], limits).unwrap())
        .unwrap();
    sender.flush(tx.as_fd()).unwrap();
    let mut receiver = FrameReceiver::new(FrameLimits {
        recv_control_fds: 0,
        ..limits
    });
    assert!(matches!(
        receiver.receive(rx.as_fd()),
        Err(FrameError::TruncatedControl)
    ));
    let flags = unsafe { libc::fcntl(read.as_raw_fd(), libc::F_GETFL) };
    unsafe { libc::fcntl(read.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) };
    let mut byte = [0];
    assert_eq!(
        unsafe { libc::read(read.as_raw_fd(), byte.as_mut_ptr().cast(), 1) },
        0,
        "Linux closes SCM_RIGHTS descriptors discarded by control truncation"
    );
}

#[test]
fn sender_eagain_before_progress_retains_ancillary_ownership() {
    let (tx, mut rx) = UnixStream::pair().unwrap();
    let fill = [0_u8; 8192];
    loop {
        let sent = unsafe {
            libc::send(
                tx.as_raw_fd(),
                fill.as_ptr().cast(),
                fill.len(),
                libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL,
            )
        };
        if sent < 0 {
            assert_eq!(
                std::io::Error::last_os_error().kind(),
                std::io::ErrorKind::WouldBlock
            );
            break;
        }
    }
    let limits = FrameLimits::default();
    let (_read, write) = pipe();
    let mut sender = FrameSender::new(limits);
    sender
        .enqueue(OutboundFrame::new(1, 1, 1, vec![], vec![write], limits).unwrap())
        .unwrap();
    assert_eq!(sender.flush(tx.as_fd()).unwrap(), FlushOutcome::WouldBlock);
    assert_eq!(sender.queued_fds(), 1);
    let mut drain = vec![0; 256 * 1024];
    let _ = rx.read(&mut drain).unwrap();
    let _ = sender.flush(tx.as_fd()).unwrap();
    assert_eq!(sender.queued_fds(), 0);
}

#[test]
fn empty_nonblocking_socket_would_block_without_state_loss() {
    let (_tx, rx) = UnixStream::pair().unwrap();
    let mut receiver = FrameReceiver::new(FrameLimits::default());
    assert!(matches!(
        receiver.receive(rx.as_fd()).unwrap(),
        ReceiveOutcome::WouldBlock
    ));
    assert_eq!(receiver.buffered_bytes(), 0);
    assert_eq!(receiver.pending_fds(), 0);
}

#[test]
fn queue_limits_reject_atomically_and_sender_remains_usable() {
    let limits = FrameLimits {
        max_queued_bytes: HEADER_LEN + 1,
        max_queued_fds: 0,
        ..FrameLimits::default()
    };
    let mut sender = FrameSender::new(limits);
    let oversized = OutboundFrame::new(1, 1, 1, b"ab".to_vec(), vec![], limits).unwrap();
    assert!(matches!(
        sender.enqueue(oversized),
        Err(FrameError::SendQueueFull { .. })
    ));
    assert!(sender.is_empty());
    let accepted = OutboundFrame::new(1, 1, 1, b"a".to_vec(), vec![], limits).unwrap();
    sender.enqueue(accepted).unwrap();
}

#[test]
fn sender_enforces_its_frame_limits_when_construction_limits_differ() {
    let construction_limits = FrameLimits {
        max_payload: 16,
        max_frame_fds: 2,
        ..FrameLimits::default()
    };
    let sender_limits = FrameLimits {
        max_payload: 1,
        max_frame_fds: 1,
        ..FrameLimits::default()
    };
    let mut sender = FrameSender::new(sender_limits);

    let payload = OutboundFrame::new(1, 1, 1, b"ab".to_vec(), vec![], construction_limits).unwrap();
    assert!(matches!(
        sender.enqueue(payload),
        Err(FrameError::PayloadTooLarge {
            declared: 2,
            limit: 1
        })
    ));
    assert!(sender.is_empty());

    let (_read1, write1) = pipe();
    let (_read2, write2) = pipe();
    let fds =
        OutboundFrame::new(1, 1, 1, vec![], vec![write1, write2], construction_limits).unwrap();
    assert!(matches!(
        sender.enqueue(fds),
        Err(FrameError::TooManyFrameFds {
            declared: 2,
            limit: 1
        })
    ));
    assert!(sender.is_empty());

    sender
        .enqueue(OutboundFrame::new(1, 1, 1, b"a".to_vec(), vec![], construction_limits).unwrap())
        .unwrap();
}

#[test]
fn partial_fd_send_commits_ancillary_once_and_resumes_bytes() {
    let (tx, rx) = UnixStream::pair().unwrap();
    let sndbuf: libc::c_int = 4096;
    assert_eq!(
        unsafe {
            libc::setsockopt(
                tx.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_SNDBUF,
                (&sndbuf as *const libc::c_int).cast(),
                std::mem::size_of_val(&sndbuf) as libc::socklen_t,
            )
        },
        0
    );
    let limits = FrameLimits {
        max_payload: 2 * 1024 * 1024,
        max_queued_bytes: 3 * 1024 * 1024,
        ..FrameLimits::default()
    };
    let (_read, write) = pipe();
    let payload = vec![0x5a; 1024 * 1024];
    let mut sender = FrameSender::new(limits);
    sender
        .enqueue(OutboundFrame::new(7, 8, 9, payload.clone(), vec![write], limits).unwrap())
        .unwrap();
    assert_eq!(sender.flush(tx.as_fd()).unwrap(), FlushOutcome::WouldBlock);
    assert!(!sender.is_empty());
    assert_eq!(
        sender.queued_fds(),
        0,
        "a positive partial send commits SCM_RIGHTS"
    );

    let mut receiver = FrameReceiver::new(limits);
    let frame = loop {
        match receiver.receive(rx.as_fd()).unwrap() {
            ReceiveOutcome::Frame(frame) => break frame,
            ReceiveOutcome::WouldBlock => {
                let _ = sender.flush(tx.as_fd()).unwrap();
            }
            ReceiveOutcome::Closed => panic!("closed during partial send"),
        }
    };
    while sender.flush(tx.as_fd()).unwrap() != FlushOutcome::Drained {}
    assert_eq!(frame.payload(), payload);
    assert_eq!(frame.fds().len(), 1);
}

#[test]
fn closed_peer_poison_sender_and_drops_queue() {
    let (tx, rx) = UnixStream::pair().unwrap();
    drop(rx);
    let limits = FrameLimits::default();
    let mut sender = FrameSender::new(limits);
    sender
        .enqueue(OutboundFrame::new(1, 1, 1, vec![], vec![], limits).unwrap())
        .unwrap();
    assert!(matches!(sender.flush(tx.as_fd()), Err(FrameError::Io(_))));
    assert!(sender.is_empty());
    assert!(matches!(
        sender.flush(tx.as_fd()),
        Err(FrameError::Poisoned)
    ));
}
