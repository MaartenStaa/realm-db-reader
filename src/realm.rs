use std::sync::Arc;
use std::{fmt::Debug, path::Path};

use anyhow::bail;
use byteorder::{ByteOrder, LittleEndian};
use memmap2::Mmap;
use tracing::instrument;

use crate::array::{Array, RealmRef};
use crate::traits::Node;
use crate::utils::read_array_value;

#[derive(Clone, Copy)]
pub(crate) struct Header {
    top_ref: [u64; 2],
    magic: [u8; 4],
    fmt_ver: [u8; 2],
    _reserved: u8,
    flags: u8,
}

impl Debug for Header {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Header")
            .field("top_ref", &self.top_ref)
            .field("fmt_ver", &self.fmt_ver)
            .field("flags", &self.flags)
            .finish()
    }
}

impl Header {
    const SIZE: usize = 24;
    const MAGIC: [u8; 4] = *b"T-DB";

    fn parse(buf: &[u8]) -> anyhow::Result<Self> {
        if buf.len() < Self::SIZE {
            bail!("file too small for Realm header");
        }

        let h = Header {
            top_ref: [
                LittleEndian::read_u64(&buf[0..8]),
                LittleEndian::read_u64(&buf[8..16]),
            ],
            magic: buf[16..20].try_into().unwrap(),
            fmt_ver: buf[20..22].try_into().unwrap(),
            _reserved: buf[22],
            flags: buf[23],
        };
        if h.magic != Self::MAGIC {
            bail!("not a Realm file (magic mismatch)");
        }

        Ok(h)
    }

    /// Choose the active top ref using the switch bit (bit 0 of `flags`).
    pub(crate) fn current_top_ref(&self) -> RealmRef {
        let idx = (self.flags & 1) as usize;
        RealmRef::new(self.top_ref[idx] as usize)
    }

    fn is_encrypted(&self) -> bool {
        self.flags & 0x80 != 0
    }

    fn file_format_version(&self) -> (u8, u8) {
        (self.fmt_ver[0], self.fmt_ver[1])
    }
}

#[derive(Clone, Copy)]
pub struct NodeHeader {
    pub checksum: u32, // 0x4141_4141 in current files
    pub flags: u8,
    pub size: u32, // 24-bit little-endian count
}

impl Debug for NodeHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NodeHeader")
            .field("is_inner_btree", &self.is_inner_bptree())
            .field("has_refs", &self.has_refs())
            .field("context_flag", &self.context_flag())
            .field("width", &self.width())
            .field("size", &self.size)
            .finish()
    }
}

pub enum NodeType {
    InnerBptree,
    HasRefs,
    Normal,
}

impl NodeHeader {
    pub const SIZE: usize = 8;
    pub const DUMMY_CHECKSUM: u32 = 0x4141_4141;

    pub fn parse(buf: &[u8]) -> anyhow::Result<Self> {
        if buf.len() < Self::SIZE {
            bail!("node too small");
        }

        let checksum = LittleEndian::read_u32(&buf[0..4]);
        let flags = buf[4];
        let size = ((buf[5] as u32) << 16) | ((buf[6] as u32) << 8) | (buf[7] as u32);

        assert_eq!(checksum, Self::DUMMY_CHECKSUM, "invalid checksum");

        Ok(Self {
            checksum,
            flags,
            size,
        })
    }

    /* flag helpers --------------------------------------------------------- */
    pub fn is_inner_bptree(&self) -> bool {
        self.flags & 0x80 != 0
    }
    pub fn has_refs(&self) -> bool {
        self.flags & 0x40 != 0
    }
    pub fn context_flag(&self) -> bool {
        self.flags & 0x20 != 0
    }
    #[inline]
    pub fn width_scheme(&self) -> u8 {
        (self.flags & 0x18) >> 3
    }
    #[inline]
    pub fn width(&self) -> u8 {
        (1 << (self.flags & 0x07)) >> 1
    }

    pub fn payload_len(&self) -> usize {
        let width = self.width() as u32;
        let num_bytes = match self.width_scheme() {
            0 => {
                // Current assumption is that size is at most 2^24 and that width is at most 64.
                // In that case the following will never overflow. (Assuming that size_t is at least 32 bits)
                assert!(self.size < 0x1000000);
                let num_bits = self.size * width;
                (num_bits + 7) >> 3
            }
            1 => self.size * width,
            2 => self.size,
            _ => {
                unreachable!("invalid width scheme");
            }
        };

        // Ensure 8-byte alignment
        // ((num_bytes + 7) & !7) as usize
        num_bytes as usize
    }

    pub fn get_type(&self) -> NodeType {
        if self.is_inner_bptree() {
            NodeType::InnerBptree
        } else if self.has_refs() {
            NodeType::HasRefs
        } else {
            NodeType::Normal
        }
    }
}

/// --- helper: decode an elem_w-sized slot into Option<ref> -------------
#[derive(Debug)]
pub(crate) enum SlotValue {
    Ref(u64),
    Inline(u64),
}

pub(crate) fn decode_slot(buf: &[u8], width: u8, index: usize) -> SlotValue {
    let v = read_array_value(buf, width, index);
    if v & 1 == 0 {
        SlotValue::Ref(v)
    } else {
        SlotValue::Inline(v >> 1)
    } // LSB clear ⇒ ref
}

pub struct Realm {
    mmap: Mmap,
    pub(crate) hdr: Header,
}

impl Debug for Realm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Realm").field("hdr", &self.hdr).finish()
    }
}

impl Realm {
    #[instrument(target = "Realm", level = "debug")]
    pub fn open(path: impl AsRef<Path> + Debug) -> anyhow::Result<Self> {
        let file = std::fs::File::open(path)?;
        let mmap = unsafe { Mmap::map(&file)? };
        let hdr = Header::parse(&mmap[..Header::SIZE])?;
        Ok(Realm { mmap, hdr })
    }

    pub(crate) fn slice(&self, ref_: RealmRef, len: usize) -> &[u8] {
        let o = ref_.to_offset();
        if o + len > self.mmap.len() {
            panic!("offset 0x{o:X} outside file");
        }
        &self.mmap[o..o + len]
    }

    pub(crate) fn payload(&self, ref_: RealmRef, payload_len: usize) -> &[u8] {
        let payload_offset = ref_ + NodeHeader::SIZE;
        self.slice(payload_offset, payload_len)
    }

    pub(crate) fn header(&self, ref_: RealmRef) -> anyhow::Result<NodeHeader> {
        let bytes = self.slice(ref_, NodeHeader::SIZE);
        NodeHeader::parse(bytes)
    }

    pub(crate) fn top_ref(&self) -> RealmRef {
        self.hdr.current_top_ref()
    }

    pub fn into_top_ref_array(self) -> anyhow::Result<Array> {
        let ref_ = self.top_ref();
        let array = Array::from_ref(Arc::new(self), ref_)?;

        Ok(array)
    }
}

#[derive(Clone)]
pub(crate) struct RealmNode {
    pub(crate) realm: Arc<Realm>,
    pub(crate) ref_: RealmRef,
    pub(crate) header: NodeHeader,
    cached_payload_len: usize,
}

impl Debug for RealmNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let payload = self.payload();

        f.debug_struct("RealmNode")
            .field("ref_", &self.ref_)
            .field("header", &self.header)
            .field(
                "payload",
                &format!(
                    "<{} byte{}>",
                    payload.len(),
                    if payload.len() == 1 { "" } else { "s" }
                ),
            )
            .finish()
    }
}

impl Node for RealmNode {
    // #[instrument(target = "RealmNode", level = "debug")]
    fn from_ref(realm: Arc<Realm>, ref_: RealmRef) -> anyhow::Result<Self> {
        let header = realm.header(ref_)?;
        let cached_payload_len = header.payload_len();

        Ok(Self {
            realm,
            ref_,
            header,
            cached_payload_len,
        })
    }
}

impl RealmNode {
    pub(crate) fn payload(&self) -> &[u8] {
        self.realm.payload(self.ref_, self.cached_payload_len)
    }
}

#[cfg(test)]
mod tests {
    use crate::realm::NodeHeader;

    #[test]
    fn test_node_header() {
        // let bytes = 0x41414141_02000002_0A000000_00000000u128.to_be_bytes();
        let bytes = [0x41, 0x41, 0x41, 0x41, 0b10, 0x00, 0x00, 0x02];
        dbg!(&bytes);
        let header = NodeHeader::parse(&bytes).unwrap();

        dbg!(&header);
        eprintln!("flags: {:08b}", header.flags);

        assert!(!header.is_inner_bptree());
        assert!(!header.has_refs());
        assert!(!header.context_flag());
        assert!(header.width_scheme() == 0);
        eprintln!("element width: {}", header.width());
        assert_eq!(header.width(), 2);
        assert_eq!(header.size, 2);
        eprintln!("payload length: {}", header.payload_len());
        // 10 bits -> 2 bytes -> align to 8
        // assert!(header.payload_len() == 8);

        // let bytes = 0x41414141_4600000Au64.to_be_bytes();
        let bytes = [0x41, 0x41, 0x41, 0x41, 0b01000110, 0x00, 0x00, 0x0A];
        dbg!(&bytes);
        let header = NodeHeader::parse(&bytes).unwrap();

        dbg!(&header);
        eprintln!(
            "flags: {:08b} width_ndx: {} width_scheme: {}",
            header.flags,
            header.width(),
            header.width_scheme()
        );

        assert!(!header.is_inner_bptree());
        assert!(header.has_refs());
        assert!(!header.context_flag());
        assert_eq!(header.width_scheme(), 0);
        eprintln!("element width: {}", header.width());
        assert_eq!(header.width(), 32);
        assert_eq!(header.size, 10);
        eprintln!("payload length: {}", header.payload_len());
        // 32 bits -> 4 bytes -> *10 = 40 -> align to 8
        assert_eq!(header.payload_len(), 40);
    }
}
