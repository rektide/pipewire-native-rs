mod wav;

use std::env;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use pipewire_native::{self as pw, core::CoreEvents, keys, properties::Properties};
#[allow(unused_imports)]
use pipewire_native_node as node;
#[allow(unused_imports)]
use pipewire_native_spa as spa;

use node::control::ControlPlaneState;

#[allow(dead_code)]
struct PlayerState {
    wav: wav::WavFile,
    position: usize,
    control_plane: ControlPlaneState,
}

impl PlayerState {
    #[allow(dead_code)]
    fn read_frames(&mut self, buf: &mut [u8], frame_size: usize) {
        let total = self.wav.data.len();
        if total == 0 || frame_size == 0 {
            return;
        }

        let mut written = 0;
        while written < buf.len() {
            let remaining = buf.len() - written;
            let available = total - self.position;
            let chunk = remaining.min(available);
            buf[written..written + chunk]
                .copy_from_slice(&self.wav.data[self.position..self.position + chunk]);
            written += chunk;
            self.position += chunk;
            if self.position >= total {
                self.position = 0;
            }
        }
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: pw-wav-player <file.wav>");
        std::process::exit(1);
    }

    let wav_path = std::path::Path::new(&args[1]);
    let wav_file = match wav::WavFile::open(wav_path) {
        Ok(w) => w,
        Err(e) => {
            eprintln!("Failed to open {}: {e}", wav_path.display());
            std::process::exit(1);
        }
    };

    eprintln!(
        "Loaded: {} Hz, {} ch, {}-bit, {} frames",
        wav_file.format.sample_rate,
        wav_file.format.channels,
        wav_file.format.bits_per_sample,
        wav_file.num_frames(),
    );

    let _player = Arc::new(std::sync::Mutex::new(PlayerState {
        wav: wav_file,
        position: 0,
        control_plane: ControlPlaneState::new(),
    }));

    pw::init();
    let mut props = Properties::new();
    props.set(keys::APP_NAME, "pw-wav-player".to_string());

    let main_loop =
        pw::thread_loop::ThreadLoop::new(&props).expect("main loop creation should not fail");
    let context = pw::context::Context::new(main_loop.main_loop(), props)
        .expect("context creation should not fail");
    let core = context.connect(None).expect("connection should succeed");

    let quit = Arc::new(AtomicBool::new(false));

    let quit_clone = quit.clone();
    core.add_listener(CoreEvents::new(
        None,
        None,
        Some(Box::new(move |_id, _seq, res, _msg| {
            let kind = std::io::Error::from_raw_os_error(res as i32).kind();
            if kind == std::io::ErrorKind::BrokenPipe {
                quit_clone.store(true, Ordering::Relaxed);
            }
        })),
    ));

    // TODO: The integration gap lives here.
    //
    // To create an audio output node, we need to call
    //   core.create_object("adapter-factory", types::interface::NODE, 3, &node_props)
    // to create a node on the server, then listen for:
    //
    //   1. Core::AddMem events — the server sends shared memory fds for audio buffers.
    //      Currently pipewire-native's core.rs closes these fds (line ~161).
    //      We need to forward them to ControlPlaneState::on_add_mem() instead.
    //
    //   2. Node param events (EnumFormat, Format, Buffers) — these describe the negotiated
    //      audio format (sample rate, channels, buffer size).
    //
    //   3. A Transport event — the server sends read/write eventfds for the process cycle.
    //      This should be fed to ControlPlaneState::on_transport().
    //
    // Once ControlPlaneState has both the memory and transport fd, we can:
    //   - Call control_plane.try_bind_transport() to get a BoundTransport
    //   - Create a NodeRuntime with the BoundTransport and our process callback
    //   - Spawn it on a Tokio runtime
    //
    // The process callback (below) would then be called once per audio quantum,
    // and we'd copy interleaved PCM from the WAV file into the shared buffer.
    //
    // --- Sketch of the process callback ---
    //
    // let player = player.clone();
    // let process_cb: node::runtime::ProcessCallback = Box::new(move |cycle| {
    //     // cycle.activation contains the SPA activation struct with buffer
    //     // offsets, status fields, etc. We'd parse it to find the audio
    //     // buffer region, then fill it from the WAV data.
    //     let _ = cycle.trigger_count;
    //     // TODO: parse activation to locate audio buffer, call
    //     //       player.lock().unwrap().read_frames(buf, frame_size)
    //     Ok(())
    // });

    eprintln!("pw-wav-player connected to PipeWire. Ctrl-C to stop.");

    main_loop.run();

    // TODO: wire quit flag into main loop wakeup so this works:
    // while !quit.load(Ordering::Relaxed) {
    //     std::thread::sleep(std::time::Duration::from_millis(100));
    // }

    main_loop.quit();
    core.disconnect();
}
