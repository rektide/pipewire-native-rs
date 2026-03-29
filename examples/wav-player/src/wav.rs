use std::fs;
use std::io::{self, Read as _};
use std::path::Path;

#[derive(Debug)]
pub struct WavFile {
    pub format: WavFormat,
    pub data: Vec<u8>,
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct WavFormat {
    pub channels: u16,
    pub sample_rate: u32,
    pub bits_per_sample: u16,
    pub block_align: u16,
}

impl WavFile {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = fs::File::open(path)?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf)?;
        Self::parse(&buf)
    }

    pub fn bytes_per_sample(&self) -> usize {
        (self.format.bits_per_sample / 8) as usize
    }

    pub fn frame_size(&self) -> usize {
        self.bytes_per_sample() * self.format.channels as usize
    }

    pub fn num_frames(&self) -> usize {
        if self.frame_size() == 0 {
            return 0;
        }
        self.data.len() / self.frame_size()
    }

    fn parse(buf: &[u8]) -> io::Result<Self> {
        if buf.len() < 44 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "WAV too short"));
        }
        if &buf[0..4] != b"RIFF" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a RIFF file",
            ));
        }
        if &buf[8..12] != b"WAVE" {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "not a WAVE file",
            ));
        }

        let mut offset = 12;
        let mut format: Option<WavFormat> = None;
        let mut data: Option<Vec<u8>> = None;

        while offset + 8 <= buf.len() {
            let chunk_id = &buf[offset..offset + 4];
            let chunk_size = u32::from_le_bytes(
                buf[offset + 4..offset + 8]
                    .try_into()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "bad chunk size"))?,
            );
            let payload_start = offset + 8;
            let payload_end = payload_start + chunk_size as usize;

            if payload_end > buf.len() {
                break;
            }

            match chunk_id {
                b"fmt " => {
                    if chunk_size < 16 {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "fmt chunk too small",
                        ));
                    }
                    let audio_format = u16::from_le_bytes(
                        buf[payload_start..payload_start + 2].try_into().unwrap(),
                    );
                    if audio_format != 1 {
                        return Err(io::Error::new(
                            io::ErrorKind::Unsupported,
                            format!("unsupported WAV format: {audio_format} (only PCM)"),
                        ));
                    }
                    let channels = u16::from_le_bytes(
                        buf[payload_start + 2..payload_start + 4]
                            .try_into()
                            .unwrap(),
                    );
                    let sample_rate = u32::from_le_bytes(
                        buf[payload_start + 4..payload_start + 8]
                            .try_into()
                            .unwrap(),
                    );
                    let block_align = u16::from_le_bytes(
                        buf[payload_start + 12..payload_start + 14]
                            .try_into()
                            .unwrap(),
                    );
                    let bits_per_sample = u16::from_le_bytes(
                        buf[payload_start + 14..payload_start + 16]
                            .try_into()
                            .unwrap(),
                    );
                    format = Some(WavFormat {
                        channels,
                        sample_rate,
                        bits_per_sample,
                        block_align,
                    });
                }
                b"data" => {
                    data = Some(buf[payload_start..payload_end].to_vec());
                }
                _ => {}
            }

            offset = payload_end;
            if chunk_size % 2 == 1 {
                offset += 1;
            }
        }

        let format = format
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing fmt chunk"))?;
        let data =
            data.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "missing data chunk"))?;

        Ok(Self { format, data })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;

    fn make_wav(channels: u16, sample_rate: u32, bits: u16, samples: &[u8]) -> Vec<u8> {
        let data_size = samples.len() as u32;
        let fmt_size = 16u32;
        let file_size = 4 + (8 + fmt_size) + (8 + data_size);

        let mut buf = Vec::new();
        buf.write_all(b"RIFF").unwrap();
        buf.extend_from_slice(&file_size.to_le_bytes());
        buf.write_all(b"WAVE").unwrap();

        buf.write_all(b"fmt ").unwrap();
        buf.extend_from_slice(&fmt_size.to_le_bytes());
        buf.extend_from_slice(&1u16.to_le_bytes());
        buf.extend_from_slice(&channels.to_le_bytes());
        buf.extend_from_slice(&sample_rate.to_le_bytes());
        let byte_rate = sample_rate * channels as u32 * (bits / 8) as u32;
        buf.extend_from_slice(&byte_rate.to_le_bytes());
        let block_align = channels * (bits / 8);
        buf.extend_from_slice(&block_align.to_le_bytes());
        buf.extend_from_slice(&bits.to_le_bytes());

        buf.write_all(b"data").unwrap();
        buf.extend_from_slice(&data_size.to_le_bytes());
        buf.extend_from_slice(samples);

        buf
    }

    #[test]
    fn parse_stereo_16bit() {
        let samples: &[u8] = &[0x01, 0x00, 0x02, 0x00, 0x03, 0x00, 0x04, 0x00];
        let wav_buf = make_wav(2, 44100, 16, samples);
        let wav = WavFile::parse(&wav_buf).unwrap();

        assert_eq!(wav.format.channels, 2);
        assert_eq!(wav.format.sample_rate, 44100);
        assert_eq!(wav.format.bits_per_sample, 16);
        assert_eq!(wav.data, samples);
        assert_eq!(wav.frame_size(), 4);
        assert_eq!(wav.num_frames(), 2);
    }

    #[test]
    fn reject_non_pcm() {
        let mut wav_buf = make_wav(1, 44100, 16, &[0x00, 0x00]);
        wav_buf[20] = 0xFF;
        wav_buf[21] = 0xFF;
        assert!(WavFile::parse(&wav_buf).is_err());
    }
}
