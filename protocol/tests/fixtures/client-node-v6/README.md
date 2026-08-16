# ClientNode v6 Upstream Fixtures

`upstream.hex` was generated with the pinned PipeWire SPA C builders from commit
`69c1b4c8b6a1cfa95982e5ed740a3995d94c1308`, using headers under
`spa/include` and the field sequences in
`src/modules/module-client-node/protocol-native.c`.

The vectors are native-endian x86_64 payload PODs. They cover `Update`,
`PortUpdate`, both `SetActive` values, `Transport`, `PortSetParam`,
`PortUseBuffers`, `PortSetIo`, `SetActivation`, and the exact SPA Node Start
command object. `format-s16le-48k-stereo` is produced by
`spa_format_audio_raw_build` with S16LE, 48000 Hz, FL/FR stereo; the same exact
object appears in `port-update` and `port-set-param`.

Sentinels are intentional: `mix_id` is `SPA_ID_INVALID` in buffer and IO
fixtures, while FD indices are frame-local `0`/`1`. Clear sentinel vectors are
covered separately by codec tests because they contain no semantic media POD.
