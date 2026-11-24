use bytes::{Buf, BufMut};

use crate::storage::tsm1::data_encoding::float::*;

// float_array_encode_all encodes src into b, returning b and any error encountered.
// The returned slice may be of a different length and capacity to b.
//
// Currently only the float compression scheme used in Facebook's Gorilla is
// supported, so this method implements a batch oriented version of that.
pub fn float_array_encode_all(src: &[f64]) -> Result<Vec<u8>> {
    let mut dst = vec![];
    let mut src = src;

    let mut pos = 0;
    dst.put_u8(FLOAT_COMPRESSED_GORILLA << 4);
    pos += 1;

    let mut first = 0.0;
    let mut finished = false;
    if !src.is_empty() && src[0].is_nan() {
        return Err(Error::StringErr {
            reason: "unsupported value: f64::NAN".to_string(),
        });
    } else if src.is_empty() {
        first = nan(); // Write sentinel value to terminate batch.
        finished = true
    } else {
        first = src[0];
        src = &src[1..];
    }

    let mut n = 8u64 + 64u64; // Number of bits written.
    let mut prev = f64::to_bits(first);

    // Write first value.
    dst.put_u64(prev);
    pos += 8;

    let (mut prev_leading, mut prev_trailing) = (!0u64, 0u64);
    let (mut leading, mut trailing) = (0u64, 0u64);
    let mut mask = 0u64;
    let mut sum = 0.0;

    // Encode remaining values.
    let mut i = 0;
    while !finished {
        let mut x = 0.0;
        if i < src.len() {
            x = src[i];
            sum += x;
        } else {
            // Encode sentinel value to terminate batch
            x = nan();
            finished = true;
        }

        let cur = f64::to_bits(x);
        let v_delta = cur ^ prev;
        if v_delta == 0 {
            n += 1; // Write a zero bit. Nothing else to do.
            prev = cur;
            i += 1;
            continue;
        }

        // First the current bit of the current byte is set to indicate we're
        // writing a delta value to the stream.
        while n >> 3 >= pos as u64 {
            // Keep growing b until we can fit all bits in.
            dst.put_u8(0u8);
            pos += 1;
        }

        // n&7 - current bit in current byte.
        // n>>3 - the current byte.
        dst[n as usize >> 3] |= 128 >> (n & 7); // Sets the current bit of the current byte.
        n += 1;

        // Write the delta to b.

        // Determine the leading and trailing zeros.
        leading = v_delta.leading_zeros() as u64;
        trailing = v_delta.trailing_zeros() as u64;

        // Clamp number of leading zeros to avoid overflow when encoding
        leading &= 0x1F;
        if leading >= 32 {
            leading = 31;
        }

        // At least 2 further bits will be required.
        while (n + 2) >> 3 >= pos as u64 {
            dst.put_u8(0u8);
            pos += 1;
        }

        if prev_leading != !0u64 && leading >= prev_leading && trailing >= prev_trailing {
            n += 1; // Write a zero bit.

            // Write the l least significant bits of v_delta to b, most significant
            // bit first.
            let l = 64 - prev_leading - prev_trailing;
            while (n + l) >> 3 >= pos as u64 {
                // Keep growing b until we can fit all bits in.
                dst.put_u8(0u8);
                pos += 1;
            }

            // Full value to write.
            let v = (v_delta >> prev_trailing) << (64 - l); // l least significant bits of v.

            let m = n & 7; // Current bit in current byte.
            let mut written = 0u64;
            if m > 0 {
                // In this case the current byte is not full.
                written = 8 - m;
                if l < written {
                    written = l;
                }
                mask = v >> 56; // Move 8 MSB to 8 LSB
                dst[n as usize >> 3] |= (mask >> m) as u8;
                n += written;

                if l - written == 0 {
                    prev = cur;
                    i += 1;
                    continue;
                }
            }

            let vv = v << written; // Move written bits out of the way.

            // TODO(edd): Optimise this. It's unlikely we actually have 8 bytes to write.
            while (n >> 3) + 8 >= pos as u64 {
                dst.put_slice(&[0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]);
                pos += 8;
            }
            (&mut dst[n as usize >> 3..(n as usize >> 3) + 8]).put_u64(vv);

            n += l - written;
        } else {
            (prev_leading, prev_trailing) = (leading, trailing);

            // Set a single bit to indicate a value will follow.
            dst[n as usize >> 3] |= 128 >> (n & 7); // Set current bit on current byte
            n += 1;

            // Write 5 bits of leading.
            while (n + 5) >> 3 >= pos as u64 {
                dst.put_u8(0u8);
                pos += 1;
            }

            // Enough room to write the 5 bits in the current byte?
            let mut m = n & 7;
            let l = 5u64;
            let v = leading << 59; // 5 LSB of leading.
            mask = v >> 56; // Move 5 MSB to 8 LSB

            if m <= 3 {
                // 5 bits fit into current byte.
                dst[n as usize >> 3] |= (mask >> m) as u8;
                n += l;
            } else {
                // In this case there are fewer than 5 bits available in current byte.
                // First step is to fill current byte
                let written = 8 - m;
                dst[n as usize >> 3] |= (mask >> m) as u8; // Some of mask will get lost.
                n += written;

                // Second step is to write the lost part of mask into the next byte.
                mask = v << written; // Move written bits in previous byte out of way.
                mask >>= 56;

                m = n & 7; // Recompute current bit.
                dst[n as usize >> 3] |= (mask >> m) as u8;
                n += l - written;
            }

            // Note that if leading == trailing == 0, then sigbits == 64.  But that
            // value doesn't actually fit into the 6 bits we have.
            // Luckily, we never need to encode 0 significant bits, since that would
            // put us in the other case (vdelta == 0).  So instead we write out a 0 and
            // adjust it back to 64 on unpacking.
            let sigbits = 64 - leading - trailing;

            while (n + 6) >> 3 >= pos as u64 {
                dst.put_u8(0u8);
                pos += 1;
            }

            m = n & 7;
            let mut l = 6u64;
            let mut v = sigbits << 58; // Move 6 LSB of sigbits to MSB
            mask = v >> 56; // Move 6 MSB to 8 LSB
            if m <= 2 {
                // The 6 bits fit into the current byte.
                dst[n as usize >> 3] |= (mask >> m) as u8;
                n += l;
            } else {
                // In this case there are fewer than 6 bits available in current byte.
                // First step is to fill the current byte.
                let written = 8 - m;
                dst[n as usize >> 3] |= (mask >> m) as u8; // Write to the current bit.
                n += written;

                // Second step is to write the lost part of mask into the next byte.
                // Write l remaining bits into current byte.
                mask = v << written; // Remove bits written in previous byte out of way.
                mask >>= 56;

                m = n & 7; // Recompute current bit.
                dst[n as usize >> 3] |= (mask >> m) as u8;
                n += l - written;
            }

            // Write final value.
            m = n & 7;
            l = sigbits;
            v = (v_delta >> trailing) << (64 - l); // Move l LSB into MSB
            while (n + l) >> 3 >= pos as u64 {
                // Keep growing b until we can fit all bits in.
                dst.put_u8(0u8);
                pos += 1;
            }

            let mut written = 0u64;
            if m > 0 {
                // In this case the current byte is not full.
                written = 8 - m;
                if l < written {
                    written = l;
                }
                mask = v >> 56; // Move 8 MSB to 8 LSB
                dst[n as usize >> 3] |= (mask >> m) as u8;
                n += written;

                if l - written == 0 {
                    prev = cur;
                    i += 1;
                    continue;
                }
            }

            // Shift remaining bits and write out in one go.
            let vv = v << written; // Remove bits written in previous byte.
            // TODO(edd): Optimise this.
            while (n >> 3) + 8 >= pos as u64 {
                dst.put_slice(&[0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8, 0u8]);
                pos += 8;
            }
            (&mut dst[n as usize >> 3..(n as usize >> 3) + 8]).put_u64(vv);

            n += l - written;
        }
        prev = cur;

        i += 1;
    }

    if sum.is_nan() {
        return Err(Error::StringErr {
            reason: "unsupported value: f64::NAN".to_string(),
        });
    }

    let mut length = n >> 3;
    if n & 7 > 0 {
        length += 1; // Add an extra byte to capture overflowing bits.
    }

    dst.truncate(length as usize);
    Ok(dst)
}

// BIT_MASK contains a lookup table where the index is the number of bits
// and the value is a mask. The table is always read by ANDing the index
// with 0x3f, such that if the index is 64, position 0 will be read, which
// is a 0xffffffffffffffff, thus returning all bits.
//
// 00 = 0xffffffffffffffff
// 01 = 0x0000000000000001
// 02 = 0x0000000000000003
// 03 = 0x0000000000000007
// ...
// 62 = 0x3fffffffffffffff
// 63 = 0x7fffffffffffffff
const BIT_MASK: [u64; 64] = [
    18446744073709551615,
    1,
    3,
    7,
    15,
    31,
    63,
    127,
    255,
    511,
    1023,
    2047,
    4095,
    8191,
    16383,
    32767,
    65535,
    131071,
    262143,
    524287,
    1048575,
    2097151,
    4194303,
    8388607,
    16777215,
    33554431,
    67108863,
    134217727,
    268435455,
    536870911,
    1073741823,
    2147483647,
    4294967295,
    8589934591,
    17179869183,
    34359738367,
    68719476735,
    137438953471,
    274877906943,
    549755813887,
    1099511627775,
    2199023255551,
    4398046511103,
    8796093022207,
    17592186044415,
    35184372088831,
    70368744177663,
    140737488355327,
    281474976710655,
    562949953421311,
    1125899906842623,
    2251799813685247,
    4503599627370495,
    9007199254740991,
    18014398509481983,
    36028797018963967,
    72057594037927935,
    144115188075855871,
    288230376151711743,
    576460752303423487,
    1152921504606846975,
    2305843009213693951,
    4611686018427387903,
    9223372036854775807,
];

pub fn float_array_decode_all(b: &[u8] /* , buf: &[f64]*/) -> Result<Vec<f64>> {
    let mut dst = Vec::new();
    float_array_decode_all_into(b, &mut dst)?;
    Ok(dst)
}

pub(crate) fn float_array_decode_all_into(b: &[u8], dst: &mut Vec<f64>) -> Result<()> {
    dst.clear();
    float_array_decode_all_with(b, |value| dst.push(value))
}

/// Uses the same checked Gorilla decoder without allocating an output array.
pub(crate) fn float_array_decode_all_with<F: FnMut(f64)>(b: &[u8], mut emit: F) -> Result<()> {
    if b.len() < 9 {
        return Ok(());
    }

    let mut val = 0u64; // current value
    let mut trailing_n = 0u8; // trailing zero count
    let mut meaningful_n = 64u8; // meaningful bit count

    // first byte is the compression type; always Gorilla
    let mut b = &b[1..];

    val = (&b[..8]).get_u64();
    if val == UVNAN {
        return Ok(());
    }

    // Keep XOR state as bits while emitting values in their original order.
    emit(f64::from_bits(val));

    b = &b[8..];

    // The bit reader code uses br_cached_val to store up to the next 8 bytes
    // of MSB data read from b. br_valid_bits stores the number of remaining unread
    // bits starting from the MSB. Before N bits are read from br_cached_val,
    // they are left-rotated N bits, such that they end up in the left-most position.
    // Using bits.RotateLeft64 results in a single instruction on many CPU architectures.
    // This approach permits simple tests, such as for the two control bits:
    //
    //    br_cached_val&1 > 0
    //
    // The alternative was to leave brCachedValue alone and perform shifts and
    // masks to read specific bits. The original approach looked like the
    // following:
    //
    //    br_cached_val&(1<<(br_valid_bits&0x3f)) > 0
    //
    let mut br_cached_val = 0u64; // a buffer of up to the next 8 bytes read from b in MSB order
    let mut br_valid_bits = 0u8; // the number of unread bits remaining in br_cached_val

    // Refill br_cached_val, reading up to 8 bytes from b
    if b.len() >= 8 {
        // fast path reads 8 bytes directly
        br_cached_val = (&b[..8]).get_u64();
        br_valid_bits = 64;
        b = &b[8..];
    } else if !b.is_empty() {
        br_cached_val = 0;
        br_valid_bits = (b.len() * 8) as u8;
        for v in b {
            br_cached_val = (br_cached_val << 8) | (*v as u64);
        }
        br_cached_val = br_cached_val.rotate_right(br_valid_bits as u32);
        b = &b[..0];
    } else {
        return Err(Error::IOError {
            reason: std::io::ErrorKind::UnexpectedEof,
        });
    }

    let mut read1 = |br_valid_bits: &mut u8,
                     br_cached_val: &mut u64,
                     meaningful_n: &mut u8,
                     b: &mut &[u8],
                     val: &mut u64| {
        // ///////////////////////////////////////////
        // //////////////READ1//////////////////////
        // ///////////////////////////////////////////
        // ///////////////////////////////////////////
        // read control bit 1
        *br_valid_bits -= 1;
        *br_cached_val = br_cached_val.rotate_left(1);
        if *br_cached_val & 1 > 0 {
            // read 5 bits for leading zero count and 6 bits for the meaningful data count
            const LEADING_TRAILING_BIT_COUNT: u8 = 11;
            let mut lm_bits = 0u64; // leading + meaningful data counts
            if *br_valid_bits >= LEADING_TRAILING_BIT_COUNT {
                // decode 5 bits leading + 6 bits meaningful for a total of 11 bits
                *br_valid_bits -= LEADING_TRAILING_BIT_COUNT;
                *br_cached_val = br_cached_val.rotate_left(LEADING_TRAILING_BIT_COUNT as u32);
                lm_bits = *br_cached_val
            } else {
                let mut bits01 = 11u8;
                if *br_valid_bits > 0 {
                    bits01 -= *br_valid_bits;
                    lm_bits = br_cached_val.rotate_left(11);
                }

                // Refill br_cached_val, reading up to 8 bytes from b
                if b.len() >= 8 {
                    *br_cached_val = (&b[..8]).get_u64();
                    *br_valid_bits = 64;
                    *b = &b[8..];
                } else if !b.is_empty() {
                    *br_cached_val = 0;
                    *br_valid_bits = (b.len() * 8) as u8;
                    for v in *b {
                        *br_cached_val = (*br_cached_val << 8) | (*v as u64);
                    }
                    *br_cached_val = br_cached_val.rotate_right(*br_valid_bits as u32);
                    *b = &b[..0];
                } else {
                    return Err(Error::IOError {
                        reason: std::io::ErrorKind::UnexpectedEof,
                    });
                }
                if *br_valid_bits < bits01 {
                    return Err(Error::IOError {
                        reason: std::io::ErrorKind::UnexpectedEof,
                    });
                }
                *br_cached_val = br_cached_val.rotate_left(bits01 as u32);
                *br_valid_bits -= bits01;
                // lm_bits &^= BIT_MASK[bits01&0x3f];
                lm_bits &= !BIT_MASK[(bits01 & 0x3f) as usize];
                lm_bits |= *br_cached_val & BIT_MASK[(bits01 & 0x3f) as usize];
            }

            lm_bits &= 0x7ff;
            let leading_n = ((lm_bits >> 6) & 0x1f) as u8; // 5 bits leading
            *meaningful_n = (lm_bits & 0x3f) as u8; // 6 bits meaningful
            if *meaningful_n > 0 {
                trailing_n = 64_u8
                    .checked_sub(leading_n)
                    .and_then(|remaining| remaining.checked_sub(*meaningful_n))
                    .ok_or_else(|| Error::StringErr {
                        reason: "invalid Gorilla significant bit range".into(),
                    })?;
            } else {
                // meaningful_n == 0 is a special case, such that all bits
                // are meaningful
                trailing_n = 0;
                *meaningful_n = 64;
            }
        }

        let mut s_bits = 0u64; // significant bits
        if *br_valid_bits >= *meaningful_n {
            *br_valid_bits -= *meaningful_n;
            *br_cached_val = br_cached_val.rotate_left(*meaningful_n as u32);
            s_bits = *br_cached_val
        } else {
            let mut m_bits = *meaningful_n;
            if *br_valid_bits > 0 {
                m_bits -= *br_valid_bits;
                s_bits = br_cached_val.rotate_left(*meaningful_n as u32);
            }

            // Refill br_cached_val, reading up to 8 bytes from b
            if b.len() >= 8 {
                *br_cached_val = (&b[..8]).get_u64();
                *br_valid_bits = 64;
                *b = &b[8..];
            } else if !b.is_empty() {
                *br_cached_val = 0;
                *br_valid_bits = (b.len() * 8) as u8;
                for v in *b {
                    *br_cached_val = (*br_cached_val << 8) | (*v as u64);
                }
                *br_cached_val = br_cached_val.rotate_right(*br_valid_bits as u32);
                *b = &b[..0];
            } else {
                return Err(Error::IOError {
                    reason: std::io::ErrorKind::UnexpectedEof,
                });
            }
            if *br_valid_bits < m_bits {
                return Err(Error::IOError {
                    reason: std::io::ErrorKind::UnexpectedEof,
                });
            }
            *br_cached_val = br_cached_val.rotate_left(m_bits as u32);
            *br_valid_bits -= m_bits;
            // s_bits &^= BIT_MASK[m_bits&0x3f];
            s_bits &= !BIT_MASK[(m_bits & 0x3f) as usize];
            s_bits |= *br_cached_val & BIT_MASK[(m_bits & 0x3f) as usize];
        }
        s_bits &= BIT_MASK[(*meaningful_n & 0x3f) as usize];

        *val ^= s_bits << (trailing_n & 0x3f);
        if *val == UVNAN {
            // IsNaN, eof
            return Err(Error::IsNAN);
        }

        Ok(())
    };

    let mut read0 = |br_valid_bits: &mut u8,
                     br_cached_val: &mut u64,
                     meaningful_n: &mut u8,
                     b: &mut &[u8],
                     emit: &mut F,
                     val: &mut u64| {
        // ///////////////////////////////////////////
        // //////////////READ0//////////////////////
        // ///////////////////////////////////////////
        // ///////////////////////////////////////////
        // read control bit 0
        *br_valid_bits -= 1;
        *br_cached_val = br_cached_val.rotate_left(1);
        if *br_cached_val & 1 > 0 {
            if *br_valid_bits > 0 {
                // goto READ1
                // read control bit 1
                read1(br_valid_bits, br_cached_val, meaningful_n, b, val)?;

                // break 'inner;
                emit(f64::from_bits(*val));
                return Ok(());
            }

            // Refill br_cached_val, reading up to 8 bytes from b
            if b.len() >= 8 {
                *br_cached_val = (&b[..8]).get_u64();
                *br_valid_bits = 64;
                *b = &b[8..];
            } else if !b.is_empty() {
                *br_cached_val = 0;
                *br_valid_bits = (b.len() * 8) as u8;
                for v in *b {
                    *br_cached_val = (*br_cached_val << 8) | (*v as u64);
                }
                *br_cached_val = br_cached_val.rotate_right(*br_valid_bits as u32);
                *b = &b[..0];
            } else {
                return Err(Error::IOError {
                    reason: std::io::ErrorKind::UnexpectedEof,
                });
            }

            read1(br_valid_bits, br_cached_val, meaningful_n, b, val)?;
        }

        emit(f64::from_bits(*val));
        Ok(())
    };

    // The expected exit condition is for a uvnan to be decoded.
    // Any other error (EOF) indicates a truncated stream.
    loop {
        if br_valid_bits > 0 {
            // br_valid_bits > 0 is impossible to predict, so we place the
            // most likely case inside the if and immediately jump, keeping
            // the instruction pipeline consistently full.
            // This is a similar approach to using the GCC __builtin_expect
            // intrinsic, which modifies the order of branches such that the
            // likely case follows the conditional jump.
            //
            // Written as if br_valid_bits == 0 and placing the Refill br_cached_val
            // code inside reduces benchmarks from 318 MB/s to 260 MB/s on an
            // Intel(R) Core(TM) i7-6920HQ CPU @ 2.90GHz

            match read0(
                &mut br_valid_bits,
                &mut br_cached_val,
                &mut meaningful_n,
                &mut b,
                &mut emit,
                &mut val,
            ) {
                Ok(()) => {}
                Err(Error::IsNAN) => break,
                Err(error) => return Err(error),
            }
            continue;
        }

        // Refill br_cached_val, reading up to 8 bytes from b
        if b.len() >= 8 {
            br_cached_val = (&b[..8]).get_u64();
            br_valid_bits = 64;
            b = &b[8..];
        } else if !b.is_empty() {
            br_cached_val = 0;
            br_valid_bits = (b.len() * 8) as u8;
            for v in b {
                br_cached_val = (br_cached_val << 8) | (*v as u64)
            }
            br_cached_val = br_cached_val.rotate_right(br_valid_bits as u32);
            b = &b[..0];
        } else {
            return Err(Error::IOError {
                reason: std::io::ErrorKind::UnexpectedEof,
            });
        }

        // ///////////////////////////////////////////
        // //////////////READ0//////////////////////
        // ///////////////////////////////////////////
        // ///////////////////////////////////////////
        match read0(
            &mut br_valid_bits,
            &mut br_cached_val,
            &mut meaningful_n,
            &mut b,
            &mut emit,
            &mut val,
        ) {
            Ok(()) => {}
            Err(Error::IsNAN) => break,
            Err(error) => return Err(error),
        }

        // // break 'inner;
        // dst.push(val);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn reused_float_decoder_rejects_truncated_stream_without_publishing_values() {
        let encoded = super::float_array_encode_all(&[1.0, 2.0, -0.0, 4.0]).unwrap();
        let mut values = vec![123.0; 16];
        for cut in 9..encoded.len() {
            assert!(
                super::float_array_decode_all_into(&encoded[..cut], &mut values).is_err(),
                "truncated stream at byte {cut}"
            );
        }
        super::float_array_decode_all_into(&encoded, &mut values).unwrap();
        assert_eq!(
            values.iter().map(|v| v.to_bits()).collect::<Vec<_>>(),
            [1.0_f64, 2.0, -0.0, 4.0]
                .iter()
                .map(|v| v.to_bits())
                .collect::<Vec<_>>()
        );
    }

    use rand::Rng;

    use super::{FloatEncoder, float_array_decode_all, float_array_encode_all};
    use crate::storage::tsm1::data_encoding::{batch_float::nan, float::tests::TWO_HOURS_DATA};

    const FULL_BLOCK_FLOAT64_ONES: [f64; 1000] = [1.0; 1000];

    #[test]
    fn test_batch_float_float_array_encode_all() {
        let examples = [
            vec![12.0, 12.0, 24.0, 13.0, 24.0, 24.0, 24.0, 24.0], // From example paper.
            vec![
                -3.8970913068231994e+307,
                -9.036931257783943e+307,
                1.7173073833490201e+308,
                -9.312369166661538e+307,
                -2.2435523083555231e+307,
                1.4779121287289644e+307,
                1.771273431601434e+308,
                8.140360378221364e+307,
                4.783405048208089e+307,
                -2.8044680049605344e+307,
                4.412915337205696e+307,
                -1.2779380602005046e+308,
                1.6235802318921885e+308,
                -1.3402901846299688e+307,
                1.6961015582104055e+308,
                -1.067980796435633e+308,
                -3.02868987458268e+307,
                1.7641793640790284e+308,
                1.6587191845856813e+307,
                -1.786073304985983e+308,
                1.0694549382051123e+308,
                3.5635180996210295e+307,
            ], // Failed during early development
            vec![
                6.00065e+06,
                6.000656e+06,
                6.000657e+06,
                6.000659e+06,
                6.000661e+06,
            ], // Similar values.
            TWO_HOURS_DATA.clone().to_vec(),
            FULL_BLOCK_FLOAT64_ONES.clone().to_vec(),
            vec![],
        ];

        for example in examples.iter() {
            let src = example.clone();
            let buf = float_array_encode_all(&src).unwrap();

            let got = float_array_decode_all(&buf).unwrap();
            assert_eq!(src, got, "expected {:?}, got {:?}", src, got);
        }
    }

    #[test]
    fn test_batch_float_float_array_encode_compare() {
        // generate random values
        let mut input = vec![];
        for _ in 0..1000 {
            input.push(rand::thread_rng().gen_range(0.0..1.0) * f64::MAX - f32::MAX as f64);
        }

        let mut enc = FloatEncoder::new();
        for v in &input {
            enc.write(*v);
        }
        enc.flush();

        let buf1 = enc.bytes().unwrap();

        let buf2 = float_array_encode_all(&input).unwrap();

        let result = float_array_decode_all(&buf2).unwrap();

        assert_eq!(
            input, result,
            "exp: {:?}
got: {:?}",
            input, result
        );

        // Check that the encoders are byte for byte the same...
        assert_eq!(buf1, &buf2, "Raw bytes differ for encoders");
    }

    #[test]
    fn test_batch_float_float_array_encode_all_nan() {
        let examples = vec![
            vec![1.0, nan(), 2.0],
            vec![1.22, nan()],
            vec![nan(), nan()],
            vec![nan()],
        ];

        for example in examples {
            assert!(
                float_array_encode_all(&example).is_err(),
                "exp error, got ok"
            );
        }
    }

    #[test]
    fn test_batch_float_float_array_encode_all_rand() {
        let mut values = vec![0f64; 50];
        for i in 0..50 {
            // note, when use (f64::MIN..=f64::MAX) will panic because of precision
            values[i] = rand::thread_rng().gen_range(-1.0e32..=-1.0e32);
        }

        values[3] = f64::MIN;
        values[33] = f64::MIN;
        values[29] = f64::MAX;
        values[41] = f64::MAX;

        let mut enc = FloatEncoder::new();
        for v in &values {
            enc.write(*v);
        }
        enc.flush();

        let buf1 = enc.bytes().unwrap();

        let buf2 = float_array_encode_all(&values).unwrap();

        let result = float_array_decode_all(&buf2).unwrap();

        assert_eq!(
            values, result,
            "exp: {:?}
got: {:?}",
            values, result
        );
    }

    #[test]
    fn test_batch_float_float_array_all_empty() {
        let mut enc = FloatEncoder::new();
        enc.flush();

        let b = enc.bytes().unwrap();
        assert!(float_array_decode_all(b).is_ok());
    }

    #[test]
    fn test_batch_float_float_array_decode_all_simple() {
        // Example from the paper
        let mut enc = FloatEncoder::new();

        let exp = vec![
            12.0, 12.0, 24.0,
            // extra tests

            // floating point masking/shifting bug
            13.0, 24.0, // delta-of-delta sizes
            24.0, 24.0, 24.0,
        ];

        for f in &exp {
            enc.write(*f);
        }
        enc.flush();

        let b = enc.bytes().unwrap();
        let got = float_array_decode_all(b).unwrap();

        assert_eq!(
            exp, got,
            "unexpected values, exp: {:?}
got: {:?}",
            exp, got
        );
    }

    #[test]
    fn test_batch_float_float_array_decode_all_empty() {
        let mut enc = FloatEncoder::new();
        enc.flush();

        let b = enc.bytes().unwrap();

        let exp = Vec::<f64>::new();
        let got = float_array_decode_all(b).unwrap();
        assert_eq!(exp, got, "unexpected values, exp empty got {:?}", got);
    }
}
