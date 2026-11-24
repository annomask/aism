use super::*;
use crate::{
    query::{
        aggregate_windows::AggregateWindows, projected_aggregate::AggregateProjection,
        storage_ir::TimeRange,
    },
    storage::tsm1::data_encoding::encoding_values::Values,
};

fn checked_payload(values: &[Value]) -> Vec<u8> {
    let payload = Values::encode(values).unwrap();
    let mut bytes = crc32fast::hash(&payload).to_be_bytes().to_vec();
    bytes.extend(payload);
    bytes
}

fn signature(states: &[crate::query::projected_aggregate::ProjectedAggregateState]) -> String {
    use crate::query::projected_aggregate::AggregateNumber;
    states
        .iter()
        .map(|state| {
            let number = |n| match n {
                Some(AggregateNumber::Float(v)) => format!("f:{}", v.to_bits()),
                n => format!("{n:?}"),
            };
            format!(
                "{:?}/{}/{}/{}/{:?}/{:?}",
                state.count,
                number(state.sum),
                number(state.min),
                number(state.max),
                state.first,
                state.last
            )
        })
        .collect::<Vec<_>>()
        .join(";")
}

#[test]
fn checked_streaming_matches_array_reduction_for_all_numeric_projections() {
    let ranges = [
        TimeRange::new(1, 12),
        TimeRange::new(20, 20),
        TimeRange::new(35, 70),
    ];
    let windows = AggregateWindows::new(&ranges).unwrap();
    for typ in [INTEGER, UNSIGNED, FLOAT] {
        let points = [0, 4, 9, 13, 20, 31, 47]
            .into_iter()
            .enumerate()
            .map(|(i, time)| match typ {
                INTEGER => Value::IntegerValue(IntegerValue::new(
                    time,
                    [i64::MIN, i64::MAX, -1, 0, 1, 2, 3][i],
                )),
                UNSIGNED => {
                    Value::UnsignedValue(UnsignedValue::new(time, [u64::MAX, 0, 1, 2, 3, 4, 5][i]))
                }
                FLOAT => Value::FloatValue(FloatValue::new(
                    time,
                    [-0.0, 0.0, 1e20, 1.0, -1e20, 2.0, 3.0][i],
                )),
                _ => unreachable!(),
            })
            .collect::<Vec<_>>();
        let bytes = checked_payload(&points);
        for mask in 0..64 {
            let projection = AggregateProjection {
                count: mask & 1 != 0,
                sum: mask & 2 != 0,
                min: mask & 4 != 0,
                max: mask & 8 != 0,
                first: mask & 16 != 0,
                last: mask & 32 != 0,
            };
            let mut initial = vec![
                crate::query::projected_aggregate::ProjectedAggregateState::empty(
                    typ, projection
                );
                3
            ];
            let values = decode(&bytes, typ).unwrap();
            values.reduce_windows(windows, &mut initial).unwrap();
            for state in &mut initial {
                if let Some(count) = &mut state.count {
                    *count = u64::MAX - 1;
                }
            }
            let mut expected = initial.clone();
            values.reduce_windows(windows, &mut expected).unwrap();
            let mut actual = initial;
            let numeric = u8::from(projection.sum)
                | (u8::from(projection.min) << 1)
                | (u8::from(projection.max) << 2);
            macro_rules! run {
                ($m:expr) => {
                    decode_reduce::<$m>(
                        &bytes,
                        &mut Vec::new(),
                        typ,
                        windows,
                        &mut actual,
                        points.len() as u64,
                        0,
                        47,
                    )
                    .unwrap()
                };
            }
            match numeric {
                0 => run!(0),
                1 => run!(1),
                2 => run!(2),
                3 => run!(3),
                4 => run!(4),
                5 => run!(5),
                6 => run!(6),
                7 => run!(7),
                _ => unreachable!(),
            }
            assert_eq!(
                signature(&actual),
                signature(&expected),
                "type {typ} projection {mask}"
            );
        }
    }
}

#[test]
fn checked_streaming_rejects_corruption_and_wrong_counts_without_committing() {
    let points = (0..16)
        .map(|i| Value::FloatValue(FloatValue::new(i * 10, i as f64 / 7.0)))
        .collect::<Vec<_>>();
    let bytes = checked_payload(&points);
    let ranges = [TimeRange::new(0, 150)];
    let windows = AggregateWindows::new(&ranges).unwrap();
    let projection = AggregateProjection {
        count: true,
        sum: true,
        min: true,
        max: true,
        ..Default::default()
    };
    let states =
        vec![crate::query::projected_aggregate::ProjectedAggregateState::empty(FLOAT, projection)];
    for cut in 5..bytes.len() {
        let mut truncated = bytes[..cut].to_vec();
        let crc = crc32fast::hash(&truncated[4..]).to_be_bytes();
        truncated[..4].copy_from_slice(&crc);
        let mut actual = states.clone();
        let valid = decode(&truncated, FLOAT).is_ok_and(|v| v.valid_range(16, 0, 150, FLOAT));
        let result = decode_reduce::<7>(
            &truncated,
            &mut Vec::new(),
            FLOAT,
            windows,
            &mut actual,
            16,
            0,
            150,
        );
        assert_eq!(result.is_ok(), valid);
        if !valid {
            assert_eq!(actual, states);
        }
    }
    for (count, start, end, typ) in [
        (15, 0, 150, FLOAT),
        (16, 1, 150, FLOAT),
        (16, 0, 149, FLOAT),
        (16, 0, 150, INTEGER),
    ] {
        let mut actual = states.clone();
        assert!(
            decode_reduce::<7>(
                &bytes,
                &mut Vec::new(),
                typ,
                windows,
                &mut actual,
                count,
                start,
                end
            )
            .is_err()
        );
        assert_eq!(actual, states);
    }
    let mut corrupt = bytes;
    corrupt[5] ^= 1;
    let mut actual = states.clone();
    assert!(
        decode_reduce::<7>(
            &corrupt,
            &mut Vec::new(),
            FLOAT,
            windows,
            &mut actual,
            16,
            0,
            150
        )
        .is_err()
    );
    assert_eq!(actual, states);
}

#[test]
fn streaming_reader_reuses_workspace_within_budget_and_bounds_window_fallback() {
    use crate::storage::tsm1::vlog::writer::VLogWriter;
    let root = tempfile::tempdir().unwrap();
    let path = root.path().join("000000001-000000001.vlog");
    let fd = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let mut writer = VLogWriter::new(fd, 1, 1, &path);
    let points = (0..100)
        .map(|i| Value::FloatValue(FloatValue::new(i, i as f64 / 7.0)))
        .collect::<Vec<_>>();
    let payload = Values::encode(&points).unwrap();
    let (offset, size) = writer.write_block(b"cpu#!~#f", 0, 99, &payload).unwrap();
    writer.close().unwrap();
    let file = Arc::new(VLogReader::new_bare(&path, 1, 1).unwrap());
    let ranges = [TimeRange::new(9, 12), TimeRange::new(17, 99)];
    let windows = AggregateWindows::new(&ranges).unwrap();
    let projection = AggregateProjection {
        count: true,
        sum: true,
        min: true,
        max: true,
        ..Default::default()
    };
    let initial =
        vec![
            crate::query::projected_aggregate::ProjectedAggregateState::empty(FLOAT, projection);
            2
        ];
    let mut expected = initial.clone();
    decode(&checked_payload(&points), FLOAT)
        .unwrap()
        .reduce_windows(windows, &mut expected)
        .unwrap();
    for budget in [0, 4096] {
        let mut reader = QueryBlockReader::new(AggregateQueryOptions {
            read_cache_bytes: budget,
            ..Default::default()
        });
        // Exercise the optional path independently of the process default.
        reader.stream_values = true;
        for _ in 0..3 {
            let mut actual = initial.clone();
            assert!(
                reader
                    .reduce_numeric(
                        &file,
                        offset as u64,
                        size as u64,
                        FLOAT,
                        windows,
                        &mut actual,
                        100,
                        0,
                        99
                    )
                    .unwrap()
            );
            assert_eq!(actual, expected);
            assert!(reader.retained_bytes() <= budget);
        }
        assert_eq!(reader.streaming_workspace.bytes() > 0, budget > 0);
        let ranges = (0..17)
            .map(|i| TimeRange::new(i * 2, i * 2))
            .collect::<Vec<_>>();
        let windows = AggregateWindows::new(&ranges).unwrap();
        let mut actual = vec![
            crate::query::projected_aggregate::ProjectedAggregateState::empty(
                FLOAT, projection
            );
            17
        ];
        let before = actual.clone();
        assert!(
            !reader
                .reduce_numeric(
                    &file,
                    offset as u64,
                    size as u64,
                    FLOAT,
                    windows,
                    &mut actual,
                    100,
                    0,
                    99
                )
                .unwrap()
        );
        assert_eq!(actual, before);
    }
}

#[test]
#[ignore = "paired decode/reduce diagnostic; run explicitly in an optimized profile"]
fn streaming_numeric_block_kernel_benchmark() {
    use std::time::Instant;
    for points in [160, 1024, 4096] {
        let values = (0..points)
            .map(|i| Value::FloatValue(FloatValue::new(i * 10, ((i * 7919) % 10007) as f64 / 8.0)))
            .collect::<Vec<_>>();
        let bytes = checked_payload(&values);
        let ranges = (0..(points + 359) / 360)
            .map(|h| TimeRange::new(h * 3600, (h + 1) * 3600 - 1))
            .collect::<Vec<_>>();
        let windows = AggregateWindows::new(&ranges).unwrap();
        let projection = AggregateProjection {
            count: true,
            sum: true,
            min: true,
            max: true,
            ..Default::default()
        };
        let initial = vec![
            crate::query::projected_aggregate::ProjectedAggregateState::empty(
                FLOAT, projection
            );
            ranges.len()
        ];
        let mut expected = initial.clone();
        decode(&bytes, FLOAT)
            .unwrap()
            .reduce_windows(windows, &mut expected)
            .unwrap();
        let mut reusable = None;
        let mut times = Vec::new();
        let mut workspace = crate::query::streaming_aggregate::StreamingWorkspace::default();
        for round_number in 0..3 {
            let modes = if round_number % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            };
            for stream in modes {
                let mut samples = Vec::new();
                for iteration in 0..1005 {
                    let mut actual = initial.clone();
                    let started = Instant::now();
                    if stream {
                        decode_reduce_reusing::<7>(
                            &bytes,
                            &mut times,
                            FLOAT,
                            windows,
                            &mut actual,
                            points as u64,
                            0,
                            (points - 1) * 10,
                            &mut workspace,
                        )
                        .unwrap();
                    } else {
                        let block = decode_reusing(&bytes, FLOAT, &mut reusable).unwrap();
                        assert!(block.valid_range(points as u64, 0, (points - 1) * 10, FLOAT));
                        block.reduce_windows(windows, &mut actual).unwrap();
                        reusable = Some(block);
                    }
                    let ms = started.elapsed().as_secs_f64() * 1000.0;
                    assert_eq!(actual, expected);
                    if iteration >= 5 {
                        samples.push(ms);
                    }
                }
                println!(
                    "STREAM_KERNEL {}",
                    serde_json::json!({
                        "points":points,"mode":if stream {"stream"} else {"array"},"round":round_number,
                        "mean_ms":samples.iter().sum::<f64>()/samples.len() as f64,
                        "samples_ms":samples,"query_workers":1,"write_workers":0,"cache":"encoded in memory",
                    })
                );
            }
        }
    }
}
