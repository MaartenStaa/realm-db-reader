use crate::array::RealmRef;
use crate::realm::{NodeHeader, Realm, SlotValue, decode_slot};

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use log::warn;

#[cfg(debug_assertions)]
#[allow(unused)]
fn indent(depth: usize) {
    print!("{:indent$}", "", indent = depth * 5);
}

#[cfg(debug_assertions)]
#[allow(unused)]
impl Realm {
    pub fn walk_tree(&self) -> Result<()> {
        self.walk(self.top_ref(), 0, None)
    }

    /// Recursively dump the tree starting at `ref_off` for demonstration.
    pub fn walk(&self, ref_: RealmRef, depth: usize, index: Option<usize>) -> Result<()> {
        // 1) parse header -------------------------------------------------
        let hdr = {
            let hbytes = self.slice(ref_, NodeHeader::SIZE);
            NodeHeader::parse(hbytes)?
        };
        let elem_w = hdr.width();
        let payload_len = hdr.payload_len();
        let payload = self.payload(ref_, payload_len);

        indent(depth);
        println!(
            "- node @ {:?}: is_inner_btree={} has_refs={} context_flag={} elem_w={elem_w} size={}",
            ref_,
            hdr.is_inner_btree(),
            hdr.has_refs(),
            hdr.context_flag(),
            hdr.size,
        );

        // How do we read the contents?
        // B+Tree Node
        if hdr.is_inner_btree() {
            assert!(
                hdr.has_refs(),
                "invariant: inner b+tree nodes must have refs"
            );

            let keys_bytes = hdr.size as usize * elem_w as usize;
            let refs_region = &payload[keys_bytes..];
            indent(depth);
            println!("  b+tree inner node, {keys_bytes} keys bytes");

            let num_refs = hdr.size as usize + 1;
            for i in 0..num_refs {
                let child_ref = LittleEndian::read_u64(&refs_region[i * 8..][..8]);

                indent(depth);
                println!("  reading ref {i} @ {child_ref:X}");

                self.walk(RealmRef::new(child_ref as usize), depth + 1, Some(i))?;
            }
            return Ok(());
        }

        if !hdr.has_refs() {
            indent(depth);
            println!(
                "  {} (no refs)",
                if hdr.is_inner_btree() {
                    "inner"
                } else {
                    "leaf"
                }
            );

            Self::print_payload(payload, elem_w, hdr.size as usize, depth);
            // indent(depth);
            // if payload_len > 60 {
            //     println!("  0x{}...", hex::encode(&payload[..60]));
            // } else if !payload.is_empty() {
            //     println!("  0x{}", hex::encode(payload));
            // } else {
            //     println!("  (empty payload)");
            // }
            //
            // leaf without refs – nothing to recurse into
            return Ok(());
        }

        assert!(!hdr.is_inner_btree());
        assert!(hdr.has_refs());

        // leaf with inline-or-ref slots ---------------------------
        for i in 0..hdr.size {
            let slot = decode_slot(payload, elem_w, i as usize);
            // dbg!(&slot);
            match slot {
                SlotValue::Ref(child_ref) => {
                    if child_ref == 0 {
                        indent(depth);
                        println!("- slot {i} is empty");
                        continue;
                    }

                    self.walk(
                        RealmRef::new(child_ref as usize),
                        depth + 1,
                        Some(i as usize),
                    )?;
                }
                SlotValue::Inline(value) => {
                    indent(depth + 1);
                    println!("- inline value: 0x{value:X} ({value})");
                }
            }
        }
        Ok(())
    }

    fn print_payload(payload: &[u8], width: u8, size: usize, depth: usize) {
        let print_integers = size > 0;
        const MAX_LEN: usize = 120;
        if print_integers {
            indent(depth);
            let start = "  integers: [";
            print!("{start}");
            let mut len = start.len();
            for i in 0..size {
                use crate::utils::read_array_value;

                if i > 0 {
                    print!(", ");
                    len += 2;
                }
                if len >= MAX_LEN {
                    print!("...");
                    break;
                }

                let value = read_array_value(payload, width, i);
                print!("{value}");
                len += value.to_string().len();
            }
            println!("]");
        }

        let print_short_strings = Self::looks_like_short_array_string(payload, width, size);
        if print_short_strings {
            indent(depth);
            let start = "  strings: [";
            print!("{start}");
            let mut len = start.len();
            for i in 0..size {
                if i > 0 {
                    print!(", ");
                    len += 2;
                }

                let item_bytes = &payload[i * width as usize..(i + 1) * width as usize];
                let num_zeroes = item_bytes[item_bytes.len() - 1] as usize;

                if num_zeroes == width as usize {
                    // indicates null value - valid
                    print!("(null string)");
                    continue;
                }

                if num_zeroes + 1 >= item_bytes.len() {
                    warn!(
                        "looks_like_array_string: index {i} has invalid string length: {}",
                        item_bytes.len()
                    );
                    continue;
                }

                if len >= MAX_LEN {
                    print!("...");
                    break;
                }

                let str_bytes = &item_bytes[..item_bytes.len() - 1 - num_zeroes];
                let s = unsafe { str::from_utf8_unchecked(str_bytes) };
                print!("{s}");
                len += s.len();
            }
            println!("]");

            // Won't print raw payload if we printed strings
            return;
        }

        indent(depth);
        let (capped_payload, capped) = if payload.len() > 60 {
            (&payload[..60], true)
        } else {
            (payload, false)
        };
        if !payload.is_empty() {
            println!(
                "  0x{}{}",
                hex::encode(payload),
                if capped { "..." } else { "" }
            );
        } else {
            println!("  (empty payload)");
            return;
        }

        if payload
            .iter()
            .all(|&c| c == 0 || (b' '..=b'~').contains(&c))
        {
            indent(depth);
            println!(
                "  maybe strings: {}{}",
                unsafe { str::from_utf8_unchecked(capped_payload) },
                if capped { "..." } else { "" }
            );
        }
    }

    fn looks_like_short_array_string(payload: &[u8], width: u8, size: usize) -> bool {
        if payload.len() < size * width as usize {
            return false;
        }

        if width == 0 {
            // all values are null, but this might not be a good indication of a string array
            return false;
        }

        for i in 0..size {
            let item_payload = &payload[i * width as usize..(i + 1) * width as usize];
            let num_zeroes = item_payload[item_payload.len() - 1] as usize;

            if num_zeroes == width as usize {
                // indicates null value - valid
                // warn!("looks_like_array_string: index {i} has null value");
                continue;
            }

            if num_zeroes + 1 >= item_payload.len() {
                // warn!(
                //     "looks_like_array_string: index {i} has invalid string length: {}",
                //     item_payload.len()
                // );
                return false;
            }

            let str_portion = &item_payload[..item_payload.len() - 1 - num_zeroes];
            if str_portion.iter().any(|&c| c < b' ') {
                // warn!(
                //     "looks_like_array_string: index {i} has invalid string content: 0x{}",
                //     hex::encode(str_portion)
                // );
                return false;
            }
        }

        true
    }
}
