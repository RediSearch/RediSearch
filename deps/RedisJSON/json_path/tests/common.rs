/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

use std::io::Read;

use serde_json::Value;

use json_path::{compile, create};

#[allow(dead_code)]
pub fn setup() {
    let _ = env_logger::try_init();
}

#[allow(dead_code)]
pub fn read_json(path: &str) -> Value {
    let mut f = std::fs::File::open(path).unwrap();
    let mut contents = String::new();
    f.read_to_string(&mut contents).unwrap();
    serde_json::from_str(&contents).unwrap()
}

#[allow(dead_code)]
#[must_use]
pub fn read_contents(path: &str) -> String {
    let mut f = std::fs::File::open(path).unwrap();
    let mut contents = String::new();
    f.read_to_string(&mut contents).unwrap();
    contents
}

#[allow(dead_code)]
pub fn select_and_then_compare(path: &str, json: Value, target: Value) {
    let json_path = compile(path).unwrap();
    let calculator = create(&json_path);
    let result = calculator.calc(&json);

    assert_eq!(
        result
            .iter()
            .map(|v| v.inner_cloned())
            .collect::<Vec<Value>>(),
        match target {
            Value::Array(vec) => vec,
            _ => panic!("Give me the Array!"),
        },
        "{path}"
    );

    // let mut selector = Selector::default();
    // let result = selector
    //     .str_path(path)
    //     .unwrap()
    //     .value(&json)
    //     .select()
    //     .unwrap();
    // assert_eq!(
    //     result.iter().map(|v| v.clone().clone()).collect::<Vec<Value>>(),
    //     match target {
    //         Value::Array(vec) => vec,
    //         _ => panic!("Give me the Array!"),
    //     },
    //     "{}",
    //     path
    // );
}

#[allow(dead_code)]
pub fn compare_result(result: Vec<&Value>, target: Value) {
    let result = serde_json::to_value(result).unwrap();
    assert_eq!(result, target);
}
