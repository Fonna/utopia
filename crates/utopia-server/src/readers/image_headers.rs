//! 只检查图片容器、宽高和动画标记，不解压像素或校验压缩数据。
//! 格式依据 PNG IHDR、JPEG T.81 frame header 和 WebP RIFF 规范。

use anyhow::anyhow;
use utopia_core::Terminal;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ImageHeader {
    pub mime: &'static str,
    pub width: u32,
    pub height: u32,
}

pub(super) fn inspect(bytes: &[u8]) -> anyhow::Result<ImageHeader> {
    if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        png(bytes)
    } else if bytes.starts_with(b"\xff\xd8") {
        jpeg(bytes)
    } else if bytes.get(..4) == Some(b"RIFF") && bytes.get(8..12) == Some(b"WEBP") {
        webp(bytes)
    } else {
        Err(anyhow!("Ark OCR supports only PNG, JPEG, WebP, or PDF input").context(Terminal))
    }
}

pub(super) fn has_signature(bytes: &[u8]) -> bool {
    bytes.starts_with(b"\x89PNG\r\n\x1a\n")
        || bytes.starts_with(b"\xff\xd8")
        || (bytes.get(..4) == Some(b"RIFF") && bytes.get(8..12) == Some(b"WEBP"))
}

fn malformed() -> anyhow::Error {
    anyhow!("The OCR image header is invalid or truncated").context(Terminal)
}

fn animated() -> anyhow::Error {
    anyhow!("The Ark OCR reader does not support animated images").context(Terminal)
}

fn part(bytes: &[u8], offset: usize, length: usize) -> anyhow::Result<&[u8]> {
    let end = offset.checked_add(length).ok_or_else(malformed)?;
    bytes.get(offset..end).ok_or_else(malformed)
}

fn be32(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().expect("a four-byte header field"))
}

fn le32(bytes: &[u8]) -> u32 {
    u32::from_le_bytes(bytes.try_into().expect("a four-byte header field"))
}

fn le24(bytes: &[u8]) -> u32 {
    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], 0])
}

fn png(bytes: &[u8]) -> anyhow::Result<ImageHeader> {
    let ihdr = part(bytes, 8, 25)?;
    if be32(&ihdr[..4]) != 13 || &ihdr[4..8] != b"IHDR" {
        return Err(malformed());
    }
    let depth = ihdr[16];
    let color = ihdr[17];
    if !matches!(
        (color, depth),
        (0, 1 | 2 | 4 | 8 | 16) | (2 | 4 | 6, 8 | 16) | (3, 1 | 2 | 4 | 8)
    ) || ihdr[18] != 0
        || ihdr[19] != 0
        || ihdr[20] > 1
    {
        return Err(malformed());
    }
    let header = ImageHeader {
        mime: "image/png",
        width: be32(&ihdr[8..12]),
        height: be32(&ihdr[12..16]),
    };
    let mut offset = 33;
    let mut image_data = false;
    while offset < bytes.len() {
        let chunk = part(bytes, offset, 8)?;
        let length = be32(&chunk[..4]) as usize;
        let total = length.checked_add(12).ok_or_else(malformed)?;
        part(bytes, offset, total)?;
        offset = offset.checked_add(total).ok_or_else(malformed)?;
        match &chunk[4..8] {
            b"acTL" | b"fcTL" | b"fdAT" => return Err(animated()),
            b"IHDR" => return Err(malformed()),
            b"IDAT" => image_data |= length > 0,
            b"IEND" if length == 0 && offset == bytes.len() && image_data => return Ok(header),
            b"IEND" => return Err(malformed()),
            _ => {}
        }
    }
    Err(malformed())
}

fn jpeg(bytes: &[u8]) -> anyhow::Result<ImageHeader> {
    // 只检查标头和末尾 EOI；熵编码流仍由供应商校验。
    if !bytes.ends_with(b"\xff\xd9") {
        return Err(malformed());
    }
    let mut offset = 2;
    let mut header = None;
    while offset < bytes.len() {
        if part(bytes, offset, 1)?[0] != 0xff {
            return Err(malformed());
        }
        while part(bytes, offset, 1)?[0] == 0xff {
            offset += 1;
        }
        let marker = part(bytes, offset, 1)?[0];
        offset += 1;
        if marker == 0x01 {
            continue; // TEM 没有长度字段。
        }
        if matches!(marker, 0x00 | 0xd0..=0xd9) {
            return Err(malformed());
        }
        let length = u16::from_be_bytes(part(bytes, offset, 2)?.try_into()?) as usize;
        if length < 2 {
            return Err(malformed());
        }
        let segment = part(bytes, offset, length)?;
        offset = offset.checked_add(length).ok_or_else(malformed)?;
        match marker {
            0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf => {
                if segment.len() < 8 || header.is_some() {
                    return Err(malformed());
                }
                let components = segment[7] as usize;
                if components == 0
                    || segment.len() != 8 + 3 * components
                    || !(1..=16).contains(&segment[2])
                {
                    return Err(malformed());
                }
                let height = u16::from_be_bytes([segment[3], segment[4]]) as u32;
                let width = u16::from_be_bytes([segment[5], segment[6]]) as u32;
                // DNL 依赖解码扫描数据才能得到高度，本读取器只接标头内有尺寸的 JPEG。
                if width == 0 || height == 0 {
                    return Err(malformed());
                }
                header = Some(ImageHeader {
                    mime: "image/jpeg",
                    width,
                    height,
                });
            }
            0xda => {
                if segment.len() < 6
                    || segment[2] == 0
                    || segment.len() != 6 + 2 * segment[2] as usize
                {
                    return Err(malformed());
                }
                return header.ok_or_else(malformed);
            }
            _ => {}
        }
    }
    Err(malformed())
}

fn webp(bytes: &[u8]) -> anyhow::Result<ImageHeader> {
    let riff = part(bytes, 0, 12)?;
    if (le32(&riff[4..8]) as usize).checked_add(8) != Some(bytes.len()) {
        return Err(malformed());
    }
    let mut offset = 12;
    let mut canvas = None;
    let mut dimensions = None;
    while offset < bytes.len() {
        let chunk = part(bytes, offset, 8)?;
        let length = le32(&chunk[4..8]) as usize;
        let total = length
            .checked_add(8)
            .and_then(|n| n.checked_add(length % 2))
            .ok_or_else(malformed)?;
        let full = part(bytes, offset, total)?;
        let payload = &full[8..8 + length];
        if length % 2 == 1 && full[total - 1] != 0 {
            return Err(malformed());
        }
        match &chunk[..4] {
            b"ANIM" | b"ANMF" => return Err(animated()),
            b"VP8X" => {
                if offset != 12 || payload.len() != 10 || payload[0] & 0xc1 != 0 {
                    return Err(malformed());
                }
                if payload[0] & 0x02 != 0 {
                    return Err(animated());
                }
                if payload[1..4] != [0, 0, 0] {
                    return Err(malformed());
                }
                canvas = Some((le24(&payload[4..7]) + 1, le24(&payload[7..10]) + 1));
            }
            b"VP8 " => {
                if dimensions.is_some()
                    || payload.len() < 10
                    || payload[0] & 1 != 0
                    || payload[3..6] != [0x9d, 0x01, 0x2a]
                {
                    return Err(malformed());
                }
                dimensions = Some((
                    (u16::from_le_bytes([payload[6], payload[7]]) & 0x3fff) as u32,
                    (u16::from_le_bytes([payload[8], payload[9]]) & 0x3fff) as u32,
                ));
            }
            b"VP8L" => {
                if dimensions.is_some() || payload.len() < 5 || payload[0] != 0x2f {
                    return Err(malformed());
                }
                let packed = le32(&payload[1..5]);
                if packed >> 29 != 0 {
                    return Err(malformed());
                }
                dimensions = Some(((packed & 0x3fff) + 1, ((packed >> 14) & 0x3fff) + 1));
            }
            _ if offset == 12 => return Err(malformed()),
            _ => {}
        }
        offset = offset.checked_add(total).ok_or_else(malformed)?;
    }
    let (width, height) = dimensions.ok_or_else(malformed)?;
    if canvas.is_some_and(|canvas| canvas != (width, height)) {
        return Err(malformed());
    }
    Ok(ImageHeader {
        mime: "image/webp",
        width,
        height,
    })
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;

    pub(crate) fn png_header(width: u32, height: u32) -> Vec<u8> {
        let mut bytes = b"\x89PNG\r\n\x1a\n".to_vec();
        let mut ihdr = Vec::new();
        ihdr.extend_from_slice(&width.to_be_bytes());
        ihdr.extend_from_slice(&height.to_be_bytes());
        ihdr.extend_from_slice(&[8, 0, 0, 0, 0]);
        png_chunk(&mut bytes, b"IHDR", &ihdr);
        png_chunk(&mut bytes, b"IDAT", &[0]);
        png_chunk(&mut bytes, b"IEND", &[]);
        bytes
    }

    fn png_chunk(bytes: &mut Vec<u8>, name: &[u8; 4], data: &[u8]) {
        bytes.extend_from_slice(&(data.len() as u32).to_be_bytes());
        bytes.extend_from_slice(name);
        bytes.extend_from_slice(data);
        bytes.extend_from_slice(&[0; 4]); // 测标头规则，不宣称压缩数据或 CRC 有效。
    }

    pub(crate) fn jpeg_header(width: u16, height: u16, progressive: bool) -> Vec<u8> {
        let mut bytes = b"\xff\xd8\xff\xe1\x00\x06Exif\xff".to_vec();
        bytes.push(if progressive { 0xc2 } else { 0xc0 });
        bytes.extend_from_slice(&[0, 11, 8]);
        bytes.extend_from_slice(&height.to_be_bytes());
        bytes.extend_from_slice(&width.to_be_bytes());
        bytes.extend_from_slice(&[1, 1, 0x11, 0]);
        bytes.extend_from_slice(b"\xff\xda\x00\x08\x01\x01\x00\x00\x3f\x00\xff\xd9");
        bytes
    }

    fn webp_chunk(name: &[u8; 4], data: &[u8]) -> Vec<u8> {
        let mut bytes = name.to_vec();
        bytes.extend_from_slice(&(data.len() as u32).to_le_bytes());
        bytes.extend_from_slice(data);
        if data.len() % 2 == 1 {
            bytes.push(0);
        }
        bytes
    }

    fn riff(chunks: &[u8]) -> Vec<u8> {
        let mut bytes = b"RIFF".to_vec();
        bytes.extend_from_slice(&(chunks.len() as u32 + 4).to_le_bytes());
        bytes.extend_from_slice(b"WEBP");
        bytes.extend_from_slice(chunks);
        bytes
    }

    pub(crate) fn webp_header(width: u32, height: u32) -> Vec<u8> {
        let mut data = vec![0x2f];
        data.extend_from_slice(&((width - 1) | ((height - 1) << 14)).to_le_bytes());
        riff(&webp_chunk(b"VP8L", &data))
    }

    #[test]
    fn reads_png_and_baseline_or_progressive_jpeg_after_metadata() {
        for bytes in [
            png_header(80, 40),
            jpeg_header(80, 40, false),
            jpeg_header(80, 40, true),
        ] {
            let header = inspect(&bytes).unwrap();
            assert_eq!((header.width, header.height), (80, 40));
        }
        assert_eq!(inspect(&png_header(80, 40)).unwrap().mime, "image/png");
        assert_eq!(
            inspect(&jpeg_header(80, 40, false)).unwrap().mime,
            "image/jpeg"
        );
    }

    #[test]
    fn reads_all_three_webp_headers_and_extended_metadata_with_odd_padding() {
        let mut vp8 = vec![0, 0, 0, 0x9d, 0x01, 0x2a];
        vp8.extend_from_slice(&80u16.to_le_bytes());
        vp8.extend_from_slice(&40u16.to_le_bytes());
        let simple = riff(&webp_chunk(b"VP8 ", &vp8));
        let lossless = webp_header(80, 40);
        let mut extended = webp_chunk(b"VP8X", &[0x08, 0, 0, 0, 79, 0, 0, 39, 0, 0]);
        extended.extend_from_slice(&webp_chunk(b"VP8 ", &vp8));
        extended.extend_from_slice(&webp_chunk(b"EXIF", b"odd"));
        for bytes in [simple, lossless, riff(&extended)] {
            assert_eq!(
                inspect(&bytes).unwrap(),
                ImageHeader {
                    mime: "image/webp",
                    width: 80,
                    height: 40
                }
            );
        }
    }

    #[test]
    fn rejects_every_truncated_header_without_panicking() {
        for bytes in [
            png_header(80, 40),
            jpeg_header(80, 40, false),
            webp_header(80, 40),
        ] {
            for length in 0..bytes.len() {
                assert!(
                    inspect(&bytes[..length]).is_err(),
                    "accepted length {length}"
                );
            }
        }
    }

    #[test]
    fn refuses_animation_even_after_image_data_and_malformed_chunk_lengths() {
        let mut png = png_header(80, 40);
        let mut animation = Vec::new();
        png_chunk(&mut animation, b"acTL", &[0; 8]);
        png.splice(46..46, animation);
        assert!(inspect(&png).is_err());
        let mut chunks = webp_chunk(b"VP8X", &[2, 0, 0, 0, 79, 0, 0, 39, 0, 0]);
        assert!(inspect(&riff(&chunks)).is_err());
        chunks = webp_header(80, 40)[12..].to_vec();
        chunks.extend_from_slice(&webp_chunk(b"ANMF", &[]));
        assert!(inspect(&riff(&chunks)).is_err());
        let mut png = png_header(80, 40);
        png[33..37].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(inspect(&png).is_err());
        let mut webp = webp_header(80, 40);
        webp[16..20].copy_from_slice(&u32::MAX.to_le_bytes());
        assert!(inspect(&webp).is_err());
    }

    #[test]
    fn rejects_invalid_jpeg_segments_and_webp_canvas_or_version() {
        for length in [0u16, 1, u16::MAX] {
            let mut jpeg = jpeg_header(80, 40, false);
            jpeg[4..6].copy_from_slice(&length.to_be_bytes());
            assert!(inspect(&jpeg).is_err());
        }
        assert!(inspect(&jpeg_header(80, 0, false)).is_err());
        let mut chunks = webp_chunk(b"VP8X", &[0, 0, 0, 0, 79, 0, 0, 39, 0, 0]);
        chunks.extend_from_slice(&webp_header(40, 80)[12..]);
        assert!(inspect(&riff(&chunks)).is_err());
        let mut webp = webp_header(80, 40);
        webp[24] |= 0x20;
        assert!(inspect(&webp).is_err());
        let mut webp = webp_header(80, 40);
        *webp.last_mut().unwrap() = 1;
        assert!(inspect(&webp).is_err());
    }
}
