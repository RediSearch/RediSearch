use super::{
    RSAggregateResult, RSNumericRecord, RSResultData, RSResultKind, RSTermRecord, RSVirtualResult,
};

#[test]
fn synced_discriminants() {
    let tests = [
        (
            RSResultData::Union(RSAggregateResult::with_capacity(0)),
            RSResultKind::Union,
        ),
        (
            RSResultData::Intersection(RSAggregateResult::with_capacity(0)),
            RSResultKind::Intersection,
        ),
        (
            RSResultData::Term(RSTermRecord::default()),
            RSResultKind::Term,
        ),
        (
            RSResultData::Virtual(RSVirtualResult),
            RSResultKind::Virtual,
        ),
        (
            RSResultData::Numeric(RSNumericRecord(0.0)),
            RSResultKind::Numeric,
        ),
        (
            RSResultData::Metric(RSNumericRecord(0.0)),
            RSResultKind::Metric,
        ),
        (
            RSResultData::HybridMetric(RSAggregateResult::with_capacity(0)),
            RSResultKind::HybridMetric,
        ),
    ];

    for (data, kind) in tests {
        assert_eq!(data.result_kind(), kind);

        let data_discriminant = unsafe { *<*const _>::from(&data).cast::<u8>() };
        let kind_discriminant = kind as u8;

        assert_eq!(data_discriminant, kind_discriminant, "for {kind:?}");
    }
}
