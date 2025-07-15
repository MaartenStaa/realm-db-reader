use byteorder::{ByteOrder, LittleEndian};

pub fn read_array_value(payload: &[u8], width: u8, index: usize) -> u64 {
    match width {
        0 => 0,
        1 => {
            let offset = index >> 3;
            ((payload[offset] >> (index & 7)) & 0x01) as u64
        }
        2 => {
            let offset = index >> 2;
            ((payload[offset] >> ((index & 3) << 1)) & 0x03) as u64
        }
        4 => {
            let offset = index >> 1;
            ((payload[offset] >> ((index & 1) << 2)) & 0x0F) as u64
        }
        8 => payload[index] as u64,
        16 => {
            let offset = index * 2;
            LittleEndian::read_u16(&payload[offset..offset + 2]) as u64
        }
        32 => {
            let offset = index * 4;
            LittleEndian::read_u32(&payload[offset..offset + 4]) as u64
        }
        64 => {
            let offset = index * 8;
            LittleEndian::read_u64(&payload[offset..offset + 8])
        }
        _ => {
            panic!("invalid width {width}");
        }
    }
}
