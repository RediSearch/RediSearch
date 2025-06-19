use std::io::{Read, Write};

use ffi::t_docId;

use crate::{Decoder, DecoderResult, Delta, Encoder, RSIndexResult, RSResultType};

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

        let num_record = unsafe { &record.data.num };

        let bytes_written = match FloatValue::from(num_record.0) {
            FloatValue::Tiny(i) => {
                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::Tiny(i),
                };

                writer.write(&[header.pack()])? + writer.write(&delta)?
            }
            FloatValue::PosInt(i) => {
                let bytes = i.to_le_bytes();
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::PositiveInteger((end - 1) as _),
                };

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(bytes)?
            }
            FloatValue::NegInt(i) => {
                let bytes = i.to_le_bytes();
                let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |pos| pos + 1);

                let bytes = &bytes[..end];

                let header = Header {
                    delta_bytes: delta.len() as _,
                    typ: HeaderType::NegativeInteger((end - 1) as _),
                };

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(bytes)?
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

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(&bytes)?
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

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(&bytes)?
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

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(&bytes)?
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

                writer.write(&[header.pack()])? + writer.write(&delta)? + writer.write(&bytes)?
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

                writer.write(&[header.pack()])? + writer.write(&delta)?
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

                writer.write(&[header.pack()])? + writer.write(&delta)?
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
        let mut header = [0u8; 1];
        let _bytes_read = reader.read(&mut header)?;
        let header = Header::unpack(header[0]);

        let mut delta = vec![0; header.delta_bytes as _];
        let _bytes_read = reader.read(&mut delta)?;
        let delta = Delta::unpack(&delta);

        let doc_id = base + (delta.0 as u64);

        let num = match header.typ {
            HeaderType::Tiny(i) => i as _,
            HeaderType::PositiveInteger(len) => {
                let mut bytes = vec![0; (len + 1) as usize];
                let _bytes_read = reader.read(&mut bytes)?;
                let mut num = 0;

                for (i, byte) in bytes.iter().enumerate() {
                    num |= (*byte as u64) << (8 * i);
                }

                num as _
            }
            HeaderType::NegativeInteger(len) => {
                let mut bytes = vec![0; (len + 1) as usize];
                let _bytes_read = reader.read(&mut bytes)?;
                let mut num = 0;

                for (i, byte) in bytes.iter().enumerate() {
                    num |= (*byte as u64) << (8 * i);
                }

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
                let mut bytes = [0u8; 4];
                let _bytes_read = reader.read(&mut bytes)?;
                let f = f32::from_le_bytes(bytes);

                f as _
            }
            HeaderType::Float {
                is_f64: false,
                is_negative: true,
                ..
            } => {
                let mut bytes = [0u8; 4];
                let _bytes_read = reader.read(&mut bytes)?;
                let f = f32::from_le_bytes(bytes) * -1.0;

                f as _
            }
            HeaderType::Float {
                is_f64: true,
                is_negative: false,
                ..
            } => {
                let mut bytes = [0u8; 8];
                let _bytes_read = reader.read(&mut bytes)?;

                f64::from_le_bytes(bytes)
            }
            HeaderType::Float {
                is_f64: true,
                is_negative: true,
                ..
            } => {
                let mut bytes = [0u8; 8];
                let _bytes_read = reader.read(&mut bytes)?;

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
        if value.fract() == 0.0 {
            if value >= 0.0 {
                let i = value as u64;

                if i <= 0b111 {
                    FloatValue::Tiny(i as u8)
                } else {
                    FloatValue::PosInt(i)
                }
            } else {
                FloatValue::NegInt((value * -1.0) as _)
            }
        } else {
            match value {
                f64::INFINITY => FloatValue::Infinity,
                f64::NEG_INFINITY => FloatValue::NegInfinity,
                v => {
                    let f32_value = v as f32;
                    let back_to_f64 = f32_value as f64;

                    if back_to_f64 == v {
                        if v < 0.0 {
                            FloatValue::F32Neg(f32_value.abs())
                        } else {
                            FloatValue::F32Pos(f32_value)
                        }
                    } else {
                        if v < 0.0 {
                            FloatValue::F64Neg(v.abs())
                        } else {
                            FloatValue::F64Pos(v)
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

    fn pack(self) -> u8 {
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

        packed
    }

    fn unpack(data: u8) -> Self {
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
