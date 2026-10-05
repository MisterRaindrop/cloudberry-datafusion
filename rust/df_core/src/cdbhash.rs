// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cloudberry's distribution hash (cdbhash.c), for routing rows through a
//! Redistribute Motion the way its PostgreSQL sender would.
//!
//! A row's hash starts at 0; for each distribution key in turn it is
//! rotated left one bit and, unless the key is NULL, xor-ed with the key's
//! hash from its operator class's hash function.  The 32-bit result picks a
//! segment with Jump Consistent Hash; in parallel mode the receiving worker
//! comes from a second jump over segments * workers (nodeMotion.c).
//!
//! Only the non-legacy hash functions of the supported types are here, each
//! a transcription of PostgreSQL's (hashfunc.c, hashfn.c).  A mismatch
//! would send rows to the wrong segment, so the C side checks this module
//! against cdbhash() value by value (datafusion_debug_cdbhash_check).

use datafusion::arrow::array::{Array, ArrayRef, AsArray};
use datafusion::arrow::datatypes::{Float32Type, Float64Type, Int16Type, Int32Type, Int64Type};

/// The hash function of a distribution key, named after the column type.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KeyHash {
    Bool,   // hashchar
    Int2,   // hashint2
    Int4,   // hashint4
    Int8,   // hashint8
    Float4, // hashfloat4
    Float8, // hashfloat8
}

impl KeyHash {
    pub fn parse(s: &str) -> Option<KeyHash> {
        Some(match s {
            "bool" => KeyHash::Bool,
            "int2" => KeyHash::Int2,
            "int4" => KeyHash::Int4,
            "int8" => KeyHash::Int8,
            "float4" => KeyHash::Float4,
            "float8" => KeyHash::Float8,
            "date" => KeyHash::Int4,
            "time" | "timestamp" | "timestamptz" => KeyHash::Int8,
            _ => return None,
        })
    }
}

#[inline]
fn final_mix(mut a: u32, mut b: u32, mut c: u32) -> u32 {
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(14));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(11));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(25));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(16));
    a ^= c;
    a = a.wrapping_sub(c.rotate_left(4));
    b ^= a;
    b = b.wrapping_sub(a.rotate_left(14));
    c ^= b;
    c = c.wrapping_sub(b.rotate_left(24));
    let _ = a;
    c
}

/// hash_bytes_uint32 (hash_uint32).
#[inline]
pub fn hash_uint32(k: u32) -> u32 {
    let init = 0x9e37_79b9u32.wrapping_add(4).wrapping_add(3_923_095);
    final_mix(init.wrapping_add(k), init, init)
}

/// hash_bytes of an aligned 8-byte key, little-endian: words lo, hi.
#[inline]
fn hash_8bytes(v: u64) -> u32 {
    let init = 0x9e37_79b9u32.wrapping_add(8).wrapping_add(3_923_095);
    final_mix(init.wrapping_add(v as u32), init.wrapping_add((v >> 32) as u32), init)
}

#[inline]
fn hash_int8(v: i64) -> u32 {
    let mut lo = v as u32;
    let hi = (v >> 32) as u32;
    lo ^= if v >= 0 { hi } else { !hi };
    hash_uint32(lo)
}

#[inline]
fn hash_float8(v: f64) -> u32 {
    if v == 0.0 {
        return 0; // 0 and -0
    }
    // get_float8_nan(): every NaN hashes as the canonical one.
    let bits = if v.is_nan() { 0x7ff8_0000_0000_0000u64 } else { v.to_bits() };
    hash_8bytes(bits)
}

/// jump_consistent_hash in cdbhash.c, including its double arithmetic.
#[inline]
pub fn jump_consistent_hash(mut key: u64, num_segments: i32) -> i32 {
    let mut b: i64 = -1;
    let mut j: i64 = 0;
    while j < num_segments as i64 {
        b = j;
        key = key.wrapping_mul(2_862_933_555_777_941_757).wrapping_add(1);
        j = ((b + 1) as f64 * ((1i64 << 31) as f64 / ((key >> 33) + 1) as f64)) as i64;
    }
    b as i32
}

/// The hash of row `r` of a key column.  None for NULL.
#[inline]
fn key_hash(h: KeyHash, a: &ArrayRef, r: usize) -> Option<u32> {
    if a.is_null(r) {
        return None;
    }
    Some(match h {
        KeyHash::Bool => hash_uint32(a.as_boolean().value(r) as u32),
        KeyHash::Int2 => hash_uint32(a.as_primitive::<Int16Type>().value(r) as i32 as u32),
        KeyHash::Int4 => hash_uint32(a.as_primitive::<Int32Type>().value(r) as u32),
        KeyHash::Int8 => hash_int8(a.as_primitive::<Int64Type>().value(r)),
        KeyHash::Float4 => hash_float8(a.as_primitive::<Float32Type>().value(r) as f64),
        KeyHash::Float8 => hash_float8(a.as_primitive::<Float64Type>().value(r)),
    })
}

/// The route of each of the `nrows` rows whose distribution keys are
/// `keys`: the receiving segment, or with `workers` >= 2 in parallel mode
/// segment * workers + worker.
pub fn routes(keys: &[(KeyHash, ArrayRef)], nrows: usize, segments: i32, workers: i32) -> Vec<u32> {
    let mut out = Vec::with_capacity(nrows);
    for r in 0..nrows {
        let mut h: u32 = 0;
        for (kh, a) in keys {
            h = h.rotate_left(1);
            if let Some(v) = key_hash(*kh, a, r) {
                h ^= v;
            }
        }
        let seg = jump_consistent_hash(h as u64, segments);
        let route = if workers >= 2 {
            let worker = jump_consistent_hash(h as u64, segments * workers) / segments;
            seg * workers + worker
        } else {
            seg
        };
        out.push(route as u32);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_values() {
        // As PostgreSQL computes them: SELECT hashint4(0), hashint4(1),
        // hashint8(-1), hashint4(-1), hashint2(-3), hashfloat8(1.5),
        // hashfloat4(1.5), hashfloat8(-0.0), hashfloat8('NaN'),
        // hashint8(5000000000), hashchar(1).
        assert_eq!(hash_uint32(0) as i32, -272711505);
        assert_eq!(hash_uint32(1) as i32, -1905060026);
        assert_eq!(hash_int8(-1) as i32, 385747274);
        assert_eq!(hash_uint32(-1i32 as u32) as i32, 385747274);
        assert_eq!(hash_uint32(-3i16 as i32 as u32) as i32, -1044245375);
        assert_eq!(hash_float8(1.5) as i32, 630860146);
        assert_eq!(hash_float8(1.5f32 as f64) as i32, 630860146);
        assert_eq!(hash_float8(-0.0), 0);
        assert_eq!(hash_float8(f64::NAN) as i32, -1275764840);
        assert_eq!(hash_float8(-f64::NAN) as i32, -1275764840);
        assert_eq!(hash_int8(5_000_000_000) as i32, -694934712);
        // Jump hash stays in range and is stable.
        for k in 0..1000u64 {
            let s = jump_consistent_hash(k, 3);
            assert!((0..3).contains(&s));
        }
    }
}
