use std::io::{Read, Write};

use ffi::t_docId;

use crate::{Decoder, DecoderResult, Delta, Encoder, RSIndexResult, RSResultType};

/// Trait to convert various types to and from byte representations for numeric encoding / decoding.
trait ToFromBytes<const N: usize> {
    /// Packs self into a byte vector.
    fn pack(self) -> [u8; N];

    /// Unpacks a byte slice into self.
    fn unpack(bytes: [u8; N]) -> Self;
}

impl ToFromBytes<{ size_of::<usize>() }> for Delta {
    fn pack(self) -> [u8; size_of::<usize>()] {
        let delta = self.0;
        delta.to_le_bytes()
    }

    fn unpack(data: [u8; size_of::<usize>()]) -> Self {
        let delta = usize::from_le_bytes(data);

        Delta(delta)
    }
}

pub struct Numeric;

impl Encoder for Numeric {
    fn encode<W: Write + std::io::Seek>(
        mut writer: W,
        delta: Delta,
        record: &RSIndexResult,
    ) -> std::io::Result<usize> {
        if matches!(
            record.result_type,
            RSResultType::Union
                | RSResultType::Intersection
                | RSResultType::Term
                | RSResultType::Virtual
                | RSResultType::HybridMetric
        ) {
            panic!("Numeric encoding only supports numeric types")
        }

        let delta = delta.pack();
        let end = delta.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);
        let delta = &delta[..end];

        let num_record = unsafe { &record.data.num };

        let bytes_written = match FloatValue::from(num_record.0) {
            FloatValue::Tiny(i) => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Tiny(i),
                };

                writer.write(&header.pack())? + writer.write(delta)?
            }
            FloatValue::PosInt(i) => {
                let bytes = i.to_le_bytes();
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::PositiveInteger((end - 1) as _),
                };

                writer.write(&header.pack())? + writer.write(delta)? + writer.write(bytes)?
            }
            FloatValue::NegInt(i) => {
                let bytes = i.to_le_bytes();
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::NegativeInteger((end - 1) as _),
                };

                writer.write(&header.pack())? + writer.write(delta)? + writer.write(bytes)?
            }
            FloatValue::F32Pos(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: false,
                        is_f64: false,
                    },
                };

                writer.write(&header.pack())? + writer.write(delta)? + writer.write(&bytes)?
            }
            FloatValue::F32Neg(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: true,
                        is_f64: false,
                    },
                };

                writer.write(&header.pack())? + writer.write(delta)? + writer.write(&bytes)?
            }
            FloatValue::F64Pos(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: false,
                        is_f64: true,
                    },
                };

                writer.write(&header.pack())? + writer.write(delta)? + writer.write(&bytes)?
            }
            FloatValue::F64Neg(value) => {
                let bytes = value.to_le_bytes();

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: false,
                        is_negative: true,
                        is_f64: true,
                    },
                };

                writer.write(&header.pack())? + writer.write(delta)? + writer.write(&bytes)?
            }
            FloatValue::Infinity => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: true,
                        is_negative: false,
                        is_f64: false,
                    },
                };

                writer.write(&header.pack())? + writer.write(delta)?
            }
            FloatValue::NegInfinity => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Float {
                        is_infinite: true,
                        is_negative: true,
                        is_f64: false,
                    },
                };

                writer.write(&header.pack())? + writer.write(delta)?
            }
        };

        Ok(bytes_written)
    }
}

impl Decoder for Numeric {
    fn decode<R: Read>(
        &self,
        mut reader: R,
        base: t_docId,
    ) -> std::io::Result<Option<DecoderResult>> {
        let mut header = [0; 1];
        reader.read_exact(&mut header)?;
        let header = Header::unpack(header);

        let mut delta = [0; size_of::<usize>()];
        reader.read_exact(&mut delta[..(header.delta_bytes) as _])?;
        let delta = Delta::unpack(delta);

        let doc_id = base + (delta.0 as u64);

        let num = match header.typ {
            HeaderType::Tiny(i) => i as _,
            HeaderType::PositiveInteger(len) => {
                let mut bytes = [0; 8];
                reader.read_exact(&mut bytes[..(len + 1) as _])?;
                let num = u64::from_le_bytes(bytes);

                num as _
            }
            HeaderType::NegativeInteger(len) => {
                let mut bytes = [0; 8];
                reader.read_exact(&mut bytes[..(len + 1) as _])?;
                let num = u64::from_le_bytes(bytes);

                (num as f64) * -1.0
            }
            HeaderType::Float {
                is_infinite: true,
                is_negative: false,
                is_f64: _,
            } => f64::INFINITY,
            HeaderType::Float {
                is_infinite: true,
                is_negative: true,
                is_f64: _,
            } => f64::NEG_INFINITY,
            HeaderType::Float {
                is_f64: false,
                is_negative: false,
                ..
            } => {
                let mut bytes = [0; 4];
                reader.read_exact(&mut bytes)?;
                let f = f32::from_le_bytes(bytes);

                f as _
            }
            HeaderType::Float {
                is_f64: false,
                is_negative: true,
                ..
            } => {
                let mut bytes = [0; 4];
                reader.read_exact(&mut bytes)?;
                let f = f32::from_le_bytes(bytes) * -1.0;

                f as _
            }
            HeaderType::Float {
                is_f64: true,
                is_negative: false,
                ..
            } => {
                let mut bytes = [0; 8];
                reader.read_exact(&mut bytes)?;

                f64::from_le_bytes(bytes)
            }
            HeaderType::Float {
                is_f64: true,
                is_negative: true,
                ..
            } => {
                let mut bytes = [0; 8];
                reader.read_exact(&mut bytes)?;

                f64::from_le_bytes(bytes) * -1.0
            }
        };
        let record = RSIndexResult::numeric(doc_id, num);

        Ok(Some(DecoderResult::Record(record)))
    }
}

enum FloatValue {
    Tiny(u8),
    PosInt(u64),
    NegInt(u64),
    F32Pos(f32),
    F32Neg(f32),
    F64Pos(f64),
    F64Neg(f64),
    Infinity,
    NegInfinity,
}

impl From<f64> for FloatValue {
    fn from(value: f64) -> Self {
        let abs_val = value.abs();
        let u64_val = abs_val as u64;

        if u64_val as f64 == abs_val {
            if u64_val <= 0b111 {
                return FloatValue::Tiny(u64_val as u8);
            } else if value.is_sign_positive() {
                return FloatValue::PosInt(u64_val);
            } else {
                return FloatValue::NegInt(u64_val);
            }
        } else {
            match value {
                f64::INFINITY => FloatValue::Infinity,
                f64::NEG_INFINITY => FloatValue::NegInfinity,
                v => {
                    let f32_value = abs_val as f32;
                    let back_to_f64 = f32_value as f64;

                    if back_to_f64 == abs_val {
                        if v.is_sign_positive() {
                            FloatValue::F32Pos(f32_value)
                        } else {
                            FloatValue::F32Neg(f32_value)
                        }
                    } else {
                        if v.is_sign_positive() {
                            FloatValue::F64Pos(abs_val)
                        } else {
                            FloatValue::F64Neg(abs_val)
                        }
                    }
                }
            }
        }
    }
}

enum HeaderType {
    Tiny(u8),
    Float {
        is_infinite: bool,
        is_negative: bool,
        is_f64: bool,
    },
    PositiveInteger(u8),
    NegativeInteger(u8),
}

struct Header {
    delta_bytes: u8,
    typ: HeaderType,
}

impl Header {
    const TINY_TYPE: u8 = 0b00;
    const FLOAT_TYPE: u8 = 0b01;
    const POS_INT_TYPE: u8 = 0b10;
    const NEG_INT_TYPE: u8 = 0b11;
}

impl ToFromBytes<1> for Header {
    fn pack(self) -> [u8; 1] {
        let mut packed = 0;
        packed |= self.delta_bytes & 0b111; // 3 bits for delta bytes

        match self.typ {
            HeaderType::Tiny(t) => {
                packed |= Self::TINY_TYPE << 3; // 2 bits for type
                packed |= (t & 0b111) << 5; // 3 bits for value
            }
            HeaderType::PositiveInteger(b) => {
                packed |= Self::POS_INT_TYPE << 3; // 2 bits for type
                packed |= (b & 0b111) << 5; // 3 bits for value bytes
            }
            HeaderType::NegativeInteger(b) => {
                packed |= Self::NEG_INT_TYPE << 3; // 2 bits for type
                packed |= (b & 0b111) << 5; // 3 bits for value bytes
            }
            HeaderType::Float {
                is_infinite,
                is_negative,
                is_f64,
            } => {
                packed |= Self::FLOAT_TYPE << 3; // 2 bits for type

                if is_infinite {
                    packed |= 1 << 5;
                }
                if is_negative {
                    packed |= 1 << 6;
                }
                if is_f64 {
                    packed |= 1 << 7;
                }
            }
        }

        [packed]
    }

    fn unpack(data: [u8; 1]) -> Self {
        let data = data[0];
        let delta_bytes = data & 0b111; // 3 bits for the delta bytes

        match (data >> 3) & 0b11 {
            Self::TINY_TYPE => {
                Self {
                    delta_bytes,
                    typ: HeaderType::Tiny(data >> 5 & 0b111), // 3 bits for the value
                }
            }
            Self::FLOAT_TYPE => {
                let is_infinite = (data >> 5) & 0b1 != 0;
                let is_negative = (data >> 6) & 0b1 != 0;
                let is_f64 = (data >> 7) & 0b1 != 0;
                Self {
                    delta_bytes,
                    typ: HeaderType::Float {
                        is_infinite,
                        is_negative,
                        is_f64,
                    },
                }
            }
            Self::POS_INT_TYPE => {
                Self {
                    delta_bytes,
                    typ: HeaderType::PositiveInteger(data >> 5 & 0b111), // 3 bits for the value bytes
                }
            }
            Self::NEG_INT_TYPE => {
                Self {
                    delta_bytes,
                    typ: HeaderType::NegativeInteger(data >> 5 & 0b111), // 3 bits for the value bytes
                }
            }
            _ => unreachable!("All four possible combinations are covered"),
        }
    }
}

#[cfg(test)]
mod tests;
