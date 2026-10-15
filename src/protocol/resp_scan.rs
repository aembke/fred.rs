//! Incremental RESP frame-boundary scanner.
//!
//! `redis_protocol`'s decoders are stateless: on a partial frame they re-parse the buffer from byte 0
//! and throw the work away. Behind a transport that yields small reads (TLS hands over one record
//! per read) that makes a large reply cost O(n²). This scanner remembers how far it got, so each
//! decode attempt only looks at the new bytes, and the full decoder runs once per complete frame.
//! It understands RESP2 and RESP3, and defers to the decoder on anything else. It only reads the
//! buffer and never copies or keeps any of it, so frames are still built by the zero-copy decoder.

/// Outcome of advancing the scan over the buffered bytes.
#[derive(Debug, PartialEq, Eq)]
pub enum Scan {
  /// A whole top-level frame is buffered, ending at the given offset.
  Complete(usize),
  /// More bytes are needed.
  Incomplete,
  /// Not a frame the scanner understands; let the full decoder decide.
  Unknown,
}

/// Stack capacity kept between frames. `Vec::clear` keeps the allocation, so without a cap one deeply
/// nested reply would keep its high-water mark allocated for the life of the connection.
const RETAINED_DEPTH: usize = 64;

#[derive(Debug, Default)]
pub struct RespScan {
  /// Offset of the next element header to read.
  pos:   usize,
  /// Elements still expected by each enclosing aggregate, innermost last.
  stack: Vec<usize>,
}

/// Parses a length or count line. RESP3 writes a streamed length as `?`, which the decoder reads as -1.
fn parse_len(line: &[u8]) -> Option<i64> {
  if line == b"?" {
    Some(-1)
  } else {
    core::str::from_utf8(line).ok()?.parse().ok()
  }
}

impl RespScan {
  pub fn reset(&mut self) {
    self.pos = 0;
    self.stack.clear();
    self.stack.shrink_to(RETAINED_DEPTH);
  }

  /// Continues the scan over `buf`, which must be the same buffer (plus appended bytes) as on the
  /// previous call, until the frame completes or the bytes run out.
  pub fn advance(&mut self, buf: &[u8]) -> Scan {
    loop {
      let start = self.pos;
      let kind = match buf.get(start) {
        Some(b) => *b,
        None => return Scan::Incomplete,
      };
      let line_end = match buf[start + 1 ..].windows(2).position(|w| w == b"\r\n") {
        Some(rel) => start + 1 + rel,
        None => return Scan::Incomplete,
      };
      let after = line_end + 2;
      let len = parse_len(&buf[start + 1 .. line_end]);

      match kind {
        // one line: simple string, simple error, number, null, boolean, double, big number, end of stream
        b'+' | b'-' | b':' | b'_' | b'#' | b',' | b'(' | b'.' => self.pos = after,
        // a length, then that many bytes: blob string, blob error, verbatim string, streamed string chunk
        b'$' | b'!' | b'=' | b';' => match len {
          // RESP2 null bulk string, RESP3 streamed string header, or the chunk that ends a streamed string
          Some(-1) if kind == b'$' => self.pos = after,
          Some(0) if kind == b';' => self.pos = after,
          Some(len) if len >= 0 => {
            let end = match after.checked_add(len as usize).and_then(|end| end.checked_add(2)) {
              Some(end) => end,
              None => return Scan::Unknown,
            };
            if buf.len() < end {
              return Scan::Incomplete;
            }
            self.pos = end;
          },
          _ => return Scan::Unknown,
        },
        // a count, then that many elements: array, set, push, map (key and value), and attribute (its
        // pairs, then the value it annotates, all decoded as one element)
        b'*' | b'~' | b'%' | b'>' | b'|' => match len {
          // RESP2 null array, or a RESP3 streamed aggregate header, which the decoder returns on its own
          Some(-1) if matches!(kind, b'*' | b'~' | b'%') => self.pos = after,
          Some(len) if len >= 0 => {
            let len = len as usize;
            let children = match kind {
              b'%' => len.checked_mul(2),
              b'|' => len.checked_mul(2).and_then(|n| n.checked_add(1)),
              _ => Some(len),
            };
            self.pos = after;
            match children {
              Some(0) => {},
              Some(n) => {
                self.stack.push(n);
                continue;
              },
              None => return Scan::Unknown,
            }
          },
          _ => return Scan::Unknown,
        },
        _ => return Scan::Unknown,
      }

      // one element finished: close every aggregate it completes
      loop {
        match self.stack.last_mut() {
          None => return Scan::Complete(self.pos),
          Some(left) => {
            *left -= 1;
            if *left > 0 {
              break;
            }
            self.stack.pop();
          },
        }
      }
    }
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use bytes::BytesMut;

  struct Rng(u64);
  impl Rng {
    fn next(&mut self) -> u64 {
      self.0 ^= self.0 << 13;
      self.0 ^= self.0 >> 7;
      self.0 ^= self.0 << 17;
      self.0
    }

    fn below(&mut self, n: u64) -> u64 {
      self.next() % n
    }
  }

  /// Bytes that must be skipped by length, never scanned: CRLF and every type byte.
  fn payload(rng: &mut Rng, len: usize) -> Vec<u8> {
    (0 .. len)
      .map(|_| b"ab\r\n*$:-+%|>~;._?"[rng.below(17) as usize])
      .collect()
  }

  fn blob(rng: &mut Rng, kind: u8, out: &mut Vec<u8>) {
    let len = rng.below(300) as usize;
    let data = payload(rng, len);
    out.push(kind);
    out.extend_from_slice(format!("{}\r\n", data.len()).as_bytes());
    out.extend_from_slice(&data);
    out.extend_from_slice(b"\r\n");
  }

  /// A hashable value, usable as a map key, set member or attribute key.
  fn key(rng: &mut Rng, out: &mut Vec<u8>) {
    match rng.below(3) {
      0 => out.extend_from_slice(format!("+k{}\r\n", rng.below(1000)).as_bytes()),
      1 => out.extend_from_slice(format!(":{}\r\n", rng.below(1000)).as_bytes()),
      _ => blob(rng, b'$', out),
    }
  }

  fn resp2(rng: &mut Rng, depth: u32, out: &mut Vec<u8>) {
    match rng.below(if depth == 0 { 6 } else { 8 }) {
      0 => out.extend_from_slice(b"+OK\r\n"),
      1 => out.extend_from_slice(b"-ERR wrong type\r\n"),
      2 => out.extend_from_slice(format!(":{}\r\n", rng.next() as i64).as_bytes()),
      3 => out.extend_from_slice(b"$-1\r\n"),
      4 | 5 => blob(rng, b'$', out),
      6 => out.extend_from_slice(if rng.below(2) == 0 { b"*0\r\n" } else { b"*-1\r\n" }),
      _ => {
        let n = 1 + rng.below(6);
        out.extend_from_slice(format!("*{}\r\n", n).as_bytes());
        for _ in 0 .. n {
          resp2(rng, depth - 1, out);
        }
      },
    }
  }

  fn resp3(rng: &mut Rng, depth: u32, out: &mut Vec<u8>) {
    match rng.below(if depth == 0 { 10 } else { 15 }) {
      0 => out.extend_from_slice(b"+OK\r\n"),
      1 => out.extend_from_slice(b"-ERR wrong type\r\n"),
      2 => out.extend_from_slice(format!(":{}\r\n", rng.next() as i64).as_bytes()),
      3 => out.extend_from_slice(b"_\r\n"),
      4 => out.extend_from_slice(if rng.below(2) == 0 { b"#t\r\n" } else { b"#f\r\n" }),
      5 => out.extend_from_slice(b",3.14159\r\n"),
      6 => out.extend_from_slice(b"(3492890328409238509324850943850943825024385\r\n"),
      7 => blob(rng, b'$', out),
      8 => blob(rng, b'!', out),
      9 => {
        let len = rng.below(300) as usize;
        let data = payload(rng, len);
        out.extend_from_slice(format!("={}\r\ntxt:", data.len() + 4).as_bytes());
        out.extend_from_slice(&data);
        out.extend_from_slice(b"\r\n");
      },
      10 | 11 => {
        let n = rng.below(5);
        out.extend_from_slice(format!("{}{}\r\n", if rng.below(2) == 0 { '*' } else { '>' }, n).as_bytes());
        for _ in 0 .. n {
          resp3(rng, depth - 1, out);
        }
      },
      12 => {
        let n = rng.below(4);
        out.extend_from_slice(format!("%{}\r\n", n).as_bytes());
        for _ in 0 .. n {
          key(rng, out);
          resp3(rng, depth - 1, out);
        }
      },
      13 => {
        let n = rng.below(4);
        out.extend_from_slice(format!("~{}\r\n", n).as_bytes());
        for _ in 0 .. n {
          key(rng, out);
        }
      },
      _ => {
        let n = rng.below(3);
        out.extend_from_slice(format!("|{}\r\n", n).as_bytes());
        for _ in 0 .. n {
          key(rng, out);
          key(rng, out);
        }
        // the annotated value
        if rng.below(2) == 0 {
          blob(rng, b'$', out);
        } else {
          let n = 1 + rng.below(3);
          out.extend_from_slice(format!("*{}\r\n", n).as_bytes());
          for _ in 0 .. n {
            resp3(rng, depth - 1, out);
          }
        }
      },
    }
  }

  /// A top-level RESP3 value, sometimes streamed: the header, each element or chunk, and the end
  /// marker then decode as separate frames.
  fn resp3_top(rng: &mut Rng, out: &mut Vec<u8>) {
    match rng.below(8) {
      0 => {
        out.extend_from_slice(b"$?\r\n");
        for _ in 0 .. rng.below(4) {
          // a chunk is never empty: `;0` ends the stream
          let len = 1 + rng.below(100) as usize;
          let data = payload(rng, len);
          out.extend_from_slice(format!(";{}\r\n", len).as_bytes());
          out.extend_from_slice(&data);
          out.extend_from_slice(b"\r\n");
        }
        out.extend_from_slice(b";0\r\n");
      },
      1 => {
        out.extend_from_slice(b"*?\r\n");
        for _ in 0 .. rng.below(4) {
          resp3(rng, 2, out);
        }
        out.extend_from_slice(b".\r\n");
      },
      2 => {
        out.extend_from_slice(b"%?\r\n");
        for _ in 0 .. rng.below(3) {
          key(rng, out);
          resp3(rng, 2, out);
        }
        out.extend_from_slice(b".\r\n");
      },
      3 => {
        out.extend_from_slice(b"~?\r\n");
        for _ in 0 .. rng.below(4) {
          key(rng, out);
        }
        out.extend_from_slice(b".\r\n");
      },
      _ => resp3(rng, 3, out),
    }
  }

  /// Feeds `wire` in random chunks, the way `Framed` does, and checks the scanner reports exactly
  /// the frame ends the full decoder finds, and never holds back a complete frame.
  fn check(rng: &mut Rng, wire: &[u8], decode: fn(&mut BytesMut) -> Option<usize>) {
    let max_chunk = [1, 7, 64, 4096][rng.below(4) as usize];
    let mut buf = BytesMut::new();
    let mut scan = RespScan::default();
    let mut off = 0;
    while off < wire.len() {
      let end = (off + 1 + rng.below(max_chunk) as usize).min(wire.len());
      buf.extend_from_slice(&wire[off .. end]);
      off = end;
      loop {
        match scan.advance(&buf) {
          Scan::Incomplete => {
            assert_eq!(decode(&mut buf.clone()), None, "held back a complete frame");
            break;
          },
          Scan::Complete(frame_end) => {
            assert_eq!(decode(&mut buf), Some(frame_end));
            scan.reset();
          },
          Scan::Unknown => panic!("valid input reported as unknown"),
        }
      }
    }
    assert!(buf.is_empty(), "frames left undecoded");
  }

  fn decode_resp2(buf: &mut BytesMut) -> Option<usize> {
    redis_protocol::resp2::decode::decode_bytes_mut(buf)
      .unwrap()
      .map(|(_, amt, _)| amt)
  }

  fn decode_resp3(buf: &mut BytesMut) -> Option<usize> {
    redis_protocol::resp3::decode::streaming::decode_bytes_mut(buf)
      .unwrap()
      .map(|(_, amt, _)| amt)
  }

  #[test]
  fn matches_resp2_decoder_on_chunked_input() {
    let mut rng = Rng(0x9e37_79b9_7f4a_7c15);
    for _ in 0 .. 3000 {
      let mut wire = Vec::new();
      for _ in 0 .. 1 + rng.below(3) {
        resp2(&mut rng, 3, &mut wire);
      }
      check(&mut rng, &wire, decode_resp2);
    }
  }

  #[test]
  fn matches_resp3_decoder_on_chunked_input() {
    let mut rng = Rng(0x2545_f491_4f6c_dd1d);
    for _ in 0 .. 3000 {
      let mut wire = Vec::new();
      for _ in 0 .. 1 + rng.below(3) {
        resp3_top(&mut rng, &mut wire);
      }
      check(&mut rng, &wire, decode_resp3);
    }
  }

  #[test]
  fn defers_to_decoder_on_unknown_input() {
    let mut scan = RespScan::default();
    assert_eq!(scan.advance(b"$abc\r\n"), Scan::Unknown);
    scan.reset();
    assert_eq!(scan.advance(b"@1\r\n"), Scan::Unknown);
    scan.reset();
    assert_eq!(scan.advance(b">?\r\n"), Scan::Unknown);
  }

  #[test]
  fn reset_releases_a_deep_stack() {
    let mut buf = b"*1\r\n".repeat(10_000);
    buf.extend_from_slice(b":1\r\n");
    let mut scan = RespScan::default();
    assert_eq!(scan.advance(&buf), Scan::Complete(buf.len()));
    assert!(scan.stack.capacity() >= 10_000);
    scan.reset();
    assert!(scan.stack.capacity() <= RETAINED_DEPTH);
  }
}
