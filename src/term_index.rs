extern crate varint;

use std::io::Cursor;
use std::str;

use key_builder::KeyBuilder;
use query::DocResult;

use rocksdb::{self, DBIterator, Snapshot, IteratorMode};
use self::varint::VarintRead;

pub struct DocResultIterator {
    iter: DBIterator,
    keypathword: String,
}

impl DocResultIterator {
    pub fn new(snapshot: &Snapshot, word: &str, kb: &KeyBuilder) -> DocResultIterator {
        DocResultIterator {
            iter: snapshot.iterator(IteratorMode::Start),
            keypathword: kb.get_keypathword_only(&word),
        }
    }

    pub fn advance_gte(&mut self, start: &DocResult) {
        KeyBuilder::add_doc_result_to_keypathword(&mut self.keypathword, &start);
        // Seek in index to >= entry
        self.iter.set_mode(IteratorMode::From(self.keypathword.as_bytes(),
                           rocksdb::Direction::Forward));
        KeyBuilder::truncate_to_keypathword(&mut self.keypathword);
    }

    pub fn next(&mut self) -> Option<(DocResult, TermPositions)> {
        if let Some((key, value)) = self.iter.next() {
            if !key.starts_with(self.keypathword.as_bytes()) {
                // we passed the key path we are interested in. nothing left to do */
                return None
            }

            let key_str = unsafe{str::from_utf8_unchecked(&key)};
            let dr = KeyBuilder::parse_doc_result_from_key(&key_str);

            Some((dr, TermPositions{pos: value.into_vec()}))
        } else {
            None
        }
    }
}


pub struct TermPositions {
    pos: Vec<u8>,
}

impl TermPositions {
    pub fn positions(self) -> Vec<u32> {
        let mut bytes = Cursor::new(self.pos);
        let mut positions = Vec::new();
        while let Ok(pos) = bytes.read_unsigned_varint_32() {
            positions.push(pos);
        }
        positions
    }
}
