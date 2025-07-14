use crate::{RSAggregateResult, RSIndexResult, RSResultType, RSResultTypeMask};

impl RSAggregateResult {
    fn get(&self, index: usize) -> Option<&RSIndexResult> {
        if index > self.num_children as _ {
            return None;
        }

        let child_ptr = unsafe { *self.children.add(index) };

        if child_ptr.is_null() {
            None
        } else {
            let child = unsafe { &*child_ptr };
            Some(child)
        }
    }
}

#[test]
fn pushing_to_aggregate_result() {
    let mut agg = RSAggregateResult::new(2);

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 0);
    assert_eq!(agg.type_mask, RSResultTypeMask::empty());

    agg.push(RSIndexResult::numeric(2, 10.0));

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 1);
    assert_eq!(agg.type_mask, RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), None, "This record does not exist yet");

    agg.push(RSIndexResult::numeric(3, 100.0));

    assert_eq!(agg.children_cap, 2);
    assert_eq!(agg.num_children, 2);
    assert_eq!(agg.type_mask, RSResultType::Numeric);

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(3, 100.0)));
    assert_eq!(agg.get(2), None, "This record does not exist yet");

    agg.push(RSIndexResult::virt(4));

    assert_eq!(agg.children_cap, 4);
    assert_eq!(agg.num_children, 3);
    assert_eq!(
        agg.type_mask,
        RSResultType::Numeric | RSResultType::Virtual,
        "types should be combined"
    );

    assert_eq!(agg.get(0), Some(&RSIndexResult::numeric(2, 10.0)));
    assert_eq!(agg.get(1), Some(&RSIndexResult::numeric(3, 100.0)));
    assert_eq!(agg.get(2), Some(&RSIndexResult::virt(4)));
    assert_eq!(agg.get(3), None, "This record does not exist yet");
}
