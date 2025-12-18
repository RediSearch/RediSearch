/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

pub mod json_node;
pub mod json_path;
pub mod select_value;

use crate::json_path::{
    CalculationResult, DummyTracker, DummyTrackerGenerator, PTracker, PTrackerGenerator,
    PathCalculator, Query, QueryCompilationError, UserPathTracker,
};
use crate::select_value::{SelectValue, ValueRef};

/// Create a `PathCalculator` object. The path calculator can be re-used
/// to calculate json paths on different JSONs.
///
/// ```
/// #[macro_use] extern crate serde_json;
///
/// use json_path;
///
/// let query = json_path::compile("$..friends[0]").unwrap();
/// let calculator = json_path::create(&query);
///
/// let json_obj = json!({
///     "school": {
///         "friends": [
///             {"name": "foo1", "age": 20},
///             {"name": "foo2", "age": 20}
///         ]
///     },
///     "friends": [
///         {"name": "foo3", "age": 30},
///         {"name": "foo4"}
/// ]});
///
/// let json = calculator.calc(&json_obj);
///
/// assert_eq!(json, vec![
///     &json!({"name": "foo3", "age": 30}),
///     &json!({"name": "foo1", "age": 20})
/// ]);
/// ```
#[must_use]
pub const fn create<'i>(query: &'i Query<'i>) -> PathCalculator<'i, DummyTrackerGenerator> {
    PathCalculator::create(query)
}

/// Create a `PathCalculator` object. The path calculator can be re-used
/// to calculate json paths on different JSONs.
/// Unlike create(), this function will return results with full path as `PTracker` object.
/// It is possible to create your own path tracker by implement the `PTrackerGenerator` trait.
#[must_use]
pub const fn create_with_generator<'i>(
    query: &'i Query<'i>,
) -> PathCalculator<'i, PTrackerGenerator> {
    PathCalculator::create_with_generator(query, PTrackerGenerator)
}

/// Compile the given json path, compilation results can after be used
/// to create `PathCalculator` calculator object to calculate json paths
pub fn compile(s: &str) -> Result<Query<'_>, QueryCompilationError> {
    json_path::compile(s)
}

/// Calc once allows to perform a one time calculation on the give query.
/// The query ownership is taken so it can not be used after. This allows
/// the get a better performance if there is a need to calculate the query
/// only once.
pub fn calc_once<'j, 'p, S: SelectValue>(q: Query<'j>, json: &'p S) -> Vec<ValueRef<'p, S>> {
    let root = q.root;
    PathCalculator::<'p, DummyTrackerGenerator> {
        query: None,
        tracker_generator: None,
    }
    .calc_with_paths_on_root(ValueRef::Borrowed(json), root)
    .into_iter()
    .map(|e: CalculationResult<'p, S, DummyTracker>| e.res)
    .collect()
}

/// A version of `calc_once` that returns also paths.
pub fn calc_once_with_paths<'p, S: SelectValue>(
    q: Query<'_>,
    json: &'p S,
) -> Vec<CalculationResult<'p, S, PTracker>> {
    let root = q.root;
    PathCalculator {
        query: None,
        tracker_generator: Some(PTrackerGenerator),
    }
    .calc_with_paths_on_root(ValueRef::Borrowed(json), root)
}

/// A version of `calc_once` that returns only paths as Vec<Vec<String>>.
pub fn calc_once_paths<S: SelectValue>(q: Query, json: &S) -> Vec<Vec<String>> {
    let root = q.root;
    PathCalculator {
        query: None,
        tracker_generator: Some(PTrackerGenerator),
    }
    .calc_with_paths_on_root(ValueRef::Borrowed(json), root)
    .into_iter()
    .map(|e| e.path_tracker.unwrap().to_string_path())
    .collect()
}

#[cfg(test)]
mod json_path_tests {
    use crate::json_path;
    use crate::{create, create_with_generator};
    use serde_json::json;
    use serde_json::Value;

    #[allow(dead_code)]
    pub fn setup() {
        let _ = env_logger::try_init();
    }

    fn perform_search<'a>(path: &str, json: &'a Value) -> Vec<Value> {
        let query = json_path::compile(path).unwrap();
        let path_calculator = create(&query);
        path_calculator
            .calc(json)
            .into_iter()
            .map(|v| v.inner_cloned())
            .collect()
    }

    fn perform_path_search(path: &str, json: &Value) -> Vec<Vec<String>> {
        let query = json_path::compile(path).unwrap();
        let path_calculator = create_with_generator(&query);
        path_calculator.calc_paths(json)
    }

    macro_rules! verify_json {(
         path: $path:expr,
         json: $json:tt,
         results: [$($result:tt),* $(,)*]
     ) => {
         let j = json!($json);
         let res = perform_search($path, &j);
         let v = vec![$(json!($result)),*];
         assert_eq!(res, v.iter().cloned().collect::<Vec<Value>>());
     }}

    macro_rules! verify_json_path {(
         path: $path:expr,
         json: $json:tt,
         results: [$([$($result:tt),*]),* $(,)*]
     ) => {
         let j = json!($json);
         let res = perform_path_search($path, &j);
         let v = vec![$(vec![$(stringify!($result),)*],)*];
         assert_eq!(res, v);
     }}

    #[test]
    fn basic1() {
        verify_json!(path:"$.foo", json:{"foo":[1,2,3]}, results:[[1,2,3]]);
    }

    #[test]
    fn basic_bracket_notation() {
        setup();
        verify_json!(path:"$[\"foo\"]", json:{"foo":[1,2,3]}, results:[[1,2,3]]);
    }

    #[test]
    fn basic_bracket_notation_with_regular_notation1() {
        verify_json!(path:"$[\"foo\"].boo", json:{"foo":{"boo":[1,2,3]}}, results:[[1,2,3]]);
    }

    #[test]
    fn basic_bracket_notation_with_regular_notation2() {
        verify_json!(path:"$.[\"foo\"].boo", json:{"foo":{"boo":[1,2,3]}}, results:[[1,2,3]]);
    }

    #[test]
    fn basic_bracket_notation_with_regular_notation3() {
        verify_json!(path:"$.foo[\"boo\"]", json:{"foo":{"boo":[1,2,3]}}, results:[[1,2,3]]);
    }

    #[test]
    fn basic_bracket_notation_with_regular_notation4() {
        verify_json!(path:"$.foo.[\"boo\"]", json:{"foo":{"boo":[1,2,3]}}, results:[[1,2,3]]);
    }

    #[test]
    fn basic_bracket_notation_with_all() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][*]", json:{"foo":{"boo":[1,2,3]}}, results:[1,2,3]);
    }

    #[test]
    fn basic_bracket_notation_with_multi_indexes() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][0,2]", json:{"foo":{"boo":[1,2,3]}}, results:[1,3]);
    }

    #[test]
    fn basic_bracket_notation_with_multi_neg_indexes() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][-3,-1]", json:{"foo":{"boo":[1,2,3]}}, results:[1,3]);
    }

    #[test]
    fn basic_full_range() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][0:2:1]", json:{"foo":{"boo":[1,2,3]}}, results:[1,2]);
        verify_json!(path:"$.foo.[\"boo\"][0:3:2]", json:{"foo":{"boo":[1,2,3]}}, results:[1,3]);
        assert!(json_path::compile("$.foo.[\"boo\"][0:3:0]").is_err());
    }

    #[test]
    fn basic_bracket_notation_with_range() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][0:2]", json:{"foo":{"boo":[1,2,3]}}, results:[1,2]);
    }

    #[test]
    fn basic_bracket_notation_with_all_range() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][:]", json:{"foo":{"boo":[1,2,3]}}, results:[1,2,3]);
    }

    #[test]
    fn basic_bracket_notation_with_right_range() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][:2]", json:{"foo":{"boo":[1,2,3]}}, results:[1,2]);
    }

    #[test]
    fn basic_bracket_notation_with_left_range() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][1:]", json:{"foo":{"boo":[1,2,3]}}, results:[2,3]);
    }

    #[test]
    fn basic_bracket_notation_with_left_range_neg() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][-2:]", json:{"foo":{"boo":[1,2,3]}}, results:[2,3]);
    }

    #[test]
    fn basic_bracket_notation_with_right_range_neg() {
        setup();
        verify_json!(path:"$.foo.[\"boo\"][:-1]", json:{"foo":{"boo":[1,2,3]}}, results:[1,2]);
    }

    #[test]
    fn basic_bracket_notation_with_multi_strings() {
        setup();
        verify_json!(path:"$.[\"foo1\",\"foo2\"].boo[0,2]", json:{"foo1":{"boo":[1,2,3]}, "foo2":{"boo":[4,5,6]}}, results:[1,3,4,6]);
    }

    #[test]
    fn basic_index1() {
        verify_json!(path:"$[\"foo\"][1]", json:{"foo":[1,2,3]}, results:[2]);
    }

    #[test]
    fn basic_index2() {
        verify_json!(path:"$[\"foo\"].[1]", json:{"foo":[1,2,3]}, results:[2]);
    }

    #[test]
    fn basic_index3() {
        verify_json!(path:"$.foo.[1]", json:{"foo":[1,2,3]}, results:[2]);
    }

    #[test]
    fn basic_index4() {
        verify_json!(path:"$.foo[1]", json:{"foo":[1,2,3]}, results:[2]);
    }

    #[test]
    fn basic_index5() {
        verify_json!(path:"$[1].foo", json:[{"foo":[1,2,3]}, {"foo":[1]}], results:[[1]]);
    }

    #[test]
    fn basic_index6() {
        verify_json!(path:"$.[1].foo", json:[{"foo":[1,2,3]}, {"foo":[1]}], results:[[1]]);
    }

    #[test]
    fn basic_index7() {
        verify_json!(path:"$[1][\"foo\"]", json:[{"foo":[1,2,3]}, {"foo":[1]}], results:[[1]]);
    }

    #[test]
    fn root_only() {
        setup();
        verify_json!(path:"$", json:{"foo":[1,2,3]}, results:[{"foo":[1,2,3]}]);
    }

    #[test]
    fn test_filter_number_eq() {
        setup();
        verify_json!(path:"$.foo[?@ == 1]", json:{"foo":[1,2,3]}, results:[1]);
    }

    #[test]
    fn test_filter_number_eq_on_literal() {
        setup();
        verify_json!(path:"$[?@.foo>=1].foo", json:[{"foo":1}], results:[1]);
    }

    #[test]
    fn test_filter_number_eq_floats() {
        setup();
        verify_json!(path:"$.foo[?@ == 1.1]", json:{"foo":[1.1,2,3]}, results:[1.1]);
    }

    #[test]
    fn test_filter_string_eq() {
        setup();
        verify_json!(path:"$.*[?@ == \"a\"]", json:{"foo":["a","b","c"], "bar":["d","e","f"]}, results:["a"]);
    }

    #[test]
    fn test_filter_number_ne() {
        setup();
        verify_json!(path:"$.*[?@ != 1]", json:{"foo":[1,2,3], "bar":[4,5,6]}, results:[2,3,4,5,6]);
    }

    #[test]
    fn test_filter_number_ne_floats() {
        setup();
        verify_json!(path:"$.*[?@ != 1.1]", json:{"foo":[1.1,2,3], "bar":[4.1,5,6]}, results:[2,3,4.1,5,6]);
    }

    #[test]
    fn test_filter_string_ne() {
        setup();
        verify_json!(path:"$.*[?@ != \"a\"]", json:{"foo":["a","b","c"], "bar":["d","e","f"]}, results:["b","c","d","e","f"]);
    }

    #[test]
    fn test_filter_number_gt() {
        setup();
        verify_json!(path:"$.*[?@ > 3]", json:{"foo":[1,2,3], "bar":[4,5,6]}, results:[4,5,6]);
    }

    #[test]
    fn test_filter_number_gt_floats() {
        setup();
        verify_json!(path:"$.*[?@ > 1.2]", json:{"foo":[1.1,2,3], "bar":[4,5,6]}, results:[2,3,4,5,6]);
    }

    #[test]
    fn test_filter_string_gt() {
        setup();
        verify_json!(path:"$.*[?@ > \"a\"]", json:{"foo":["a","b","c"], "bar":["d","e","f"]}, results:["b","c","d","e","f"]);
    }

    #[test]
    fn test_filter_number_ge() {
        setup();
        verify_json!(path:"$.*[?@ >= 3]", json:{"foo":[1,2,3], "bar":[4,5,6]}, results:[3,4,5,6]);
    }

    #[test]
    fn test_filter_number_ge_floats() {
        setup();
        verify_json!(path:"$.*[?@ >= 3.1]", json:{"foo":[1,2,3.1], "bar":[4,5,6]}, results:[3.1,4,5,6]);
    }

    #[test]
    fn test_filter_string_ge() {
        setup();
        verify_json!(path:"$.*[?@ >= \"a\"]", json:{"foo":["a","b","c"], "bar":["d","e","f"]}, results:["a", "b", "c", "d", "e", "f"]);
    }

    #[test]
    fn test_filter_number_lt() {
        setup();
        verify_json!(path:"$.*[?@ < 4]", json:{"foo":[1,2,3], "bar":[4,5,6]}, results:[1,2,3]);
    }

    #[test]
    fn test_filter_number_lt_floats() {
        setup();
        verify_json!(path:"$.*[?@ < 3.9]", json:{"foo":[1,2,3], "bar":[3,5,6.9]}, results:[1,2,3,3]);
    }

    #[test]
    fn test_filter_string_lt() {
        setup();
        verify_json!(path:"$.*[?@ < \"d\"]", json:{"foo":["a","b","c"], "bar":["d","e","f"]}, results:["a", "b", "c"]);
    }

    #[test]
    fn test_filter_number_le() {
        setup();
        verify_json!(path:"$.*[?@ <= 6]", json:{"foo":[1,2,3], "bar":[4,5,6]}, results:[1,2,3,4,5,6]);
    }

    #[test]
    fn test_filter_number_le_floats() {
        setup();
        verify_json!(path:"$.*[?@ <= 6.1]", json:{"foo":[1,2,3], "bar":[4,5,6]}, results:[1,2,3,4,5,6]);
    }

    #[test]
    fn test_filter_string_le() {
        setup();
        verify_json!(path:"$.*[?@ <= \"d\"]", json:{"foo":["a","b","c"], "bar":["d","e","f"]}, results:["a", "b", "c", "d"]);
    }

    #[test]
    fn test_filter_and() {
        setup();
        verify_json!(path:"$[?@.foo[0] == 1 && @foo[1] == 2].foo[0,1,2]", json:[{"foo":[1,2,3], "bar":[4,5,6]}], results:[1,2,3]);
    }

    #[test]
    fn test_filter_and_three() {
        setup();
        verify_json!(path:"$[?@.foo[0] == 1 && @foo[1] == 2 && @foo[2] == 0]", json:[{"foo":[1,2,3], "bar":[4,5,6]}], results:[]);
    }

    #[test]
    fn test_filter_and_four() {
        setup();
        verify_json!(path:"$[?@.foo[0] == 1 && @foo[1] == 2 && @foo[2] == 2 && @foo[3] == 0]", json:[{"foo":[1,2,3,4], "bar":[4,5,6]}], results:[]);
    }

    #[test]
    fn test_filter_and_four_obj() {
        setup();
        verify_json!(path:"$[?(@.foo>1 && @.quux>8 && @.bar>3 && @.baz>4)]",
             json:[{"foo":1, "bar":2, "baz": 3, "quux": 4}, {"foo":2, "bar":4, "baz": 6, "quux": 9}, {"foo":2, "bar":3, "baz": 6, "quux": 10}],
             results:[{"foo":2, "bar":4, "baz": 6, "quux": 9}]);
    }

    #[test]
    fn test_filter_or() {
        setup();
        verify_json!(path:"$[?@.foo[0] == 2 || @.bar[0] == 4].*[0,1,2]", json:[{"foo":[1,2,3], "bar":[4,5,6]}], results:[1,2,3,4,5,6]);
    }

    #[test]
    fn test_filter_or_three() {
        setup();
        verify_json!(path:"$[?@.foo[0] == 0 || @.bar[0] == 0 || @.foo[1] == 0 || @.bar[0] == 4 ].*[0,1,2]",
             json:[{"foo":[1,2,3], "bar":[4,5,6]}],
             results:[1,2,3,4,5,6]);
    }

    #[test]
    fn test_filter_or_four() {
        setup();
        verify_json!(path:"$[?@.foo[0] == 2 || @.bar[0] == 4].*[0,1,2]", json:[{"foo":[1,2,3], "bar":[4,5,6]}], results:[1,2,3,4,5,6]);
    }

    #[test]
    fn test_complex_filter() {
        setup();
        verify_json!(path:"$[?(@.foo[0] == 1 && @.foo[2] == 3)||(@.bar[0]==4&&@.bar[2]==6)].*[0,1,2]", json:[{"foo":[1,2,3], "bar":[4,5,6]}], results:[1,2,3,4,5,6]);
    }

    #[test]
    fn test_complex_filter_precedence() {
        setup();
        let json = json!([{"t":true, "f":false, "one":1}, {"t":true, "f":false, "one":3}]);
        verify_json!(path:"$[?(@.f==true || @.one==1 && @.t==false)]", json:json, results:[]);
        verify_json!(path:"$[?(@.f==true || @.one==1 && @.t==true)].*", json:json, results:[true, false, 1]);
        verify_json!(path:"$[?(@.t==true && @.one==1 || @.t==true)].*", json:json, results:[true, false, 1, true, false, 3]);

        // With A=False, B=False, C=True
        // "(A && B) || C"  ==> True
        // "A && (B  || C)" ==> False
        verify_json!(path:"$[?(@.f==true &&  @.t==false || @.one==1)].*", json:json, results:[true, false, 1]);
        verify_json!(path:"$[?(@.f==true && (@.t==false || @.one==1))].*", json:json, results:[]);
    }

    #[test]
    fn test_complex_filter_nesting() {
        setup();
        let json = json!([{"t":true, "f":false, "one":1}, {"t":true, "f":false, "one":3}]);
        // With A=False, B=False, C=True
        // "(A && B) || C"  ==> True
        // "A && (B  || C)" ==> False
        verify_json!(path:"$[?(@.f==true &&  (@.f==true || (@.t==true && (@.one>1 && @.f==true))) || ((@.one==2 || @.one==1) && @.t==true))].*", json:json, results:[true, false, 1]);
        verify_json!(path:"$[?(@.f==true &&  ((@.f==true || (@.t==true && (@.one>1 && @.f==true))) || ((@.one==2 || @.one==1) && @.t==true)))].*", json:json, results:[]);
    }

    #[test]
    fn test_filter_with_full_scan() {
        setup();
        verify_json!(path:"$..[?(@.code==\"2\")].code", json:[{"code":"1"},{"code":"2"}], results:["2"]);
    }

    #[test]
    fn test_full_scan_with_all() {
        setup();
        verify_json!(path:"$..*.*", json:[{"code":"1"},{"code":"2"}], results:["1", "2"]);
    }

    #[test]
    fn test_filter_with_all() {
        setup();
        verify_json!(path:"$.*.[?(@.code==\"2\")].code", json:[[{"code":"1"},{"code":"2"}]], results:["2"]);
        verify_json!(path:"$.*[?(@.code==\"2\")].code", json:[[{"code":"1"},{"code":"2"}]], results:["2"]);
        verify_json!(path:"$*[?(@.code==\"2\")].code", json:[[{"code":"1"},{"code":"2"}]], results:["2"]);
    }

    #[test]
    fn test_filter_bool() {
        setup();
        verify_json!(path:"$.*[?(@==true)]", json:{"a":true, "b":false}, results:[true]);
        verify_json!(path:"$.*[?(@==false)]", json:{"a":true, "b":false}, results:[false]);
    }

    #[test]
    fn test_filter_null() {
        setup();
        verify_json!(path:"$.*[?(@==null)]", json:{"a":null}, results:[null]);
        verify_json!(path:"$[?(@.*==null)]", json:[{"a":10}, {"b":null}, {"c":30}], results:[{"b": null}]);
    }

    #[test]
    fn test_complex_filter_from_root() {
        setup();
        verify_json!(path:"$.bar.*[?@ == $.foo]",
                      json:{"foo":1, "bar":{"a":[1,2,3], "b":[4,5,6]}},
                      results:[1]);
    }

    #[test]
    fn test_complex_filter_with_literal() {
        setup();
        verify_json!(path:"$.foo[?@.a == @.b].boo[:]",
                      json:{"foo":[{"boo":[1,2,3],"a":1,"b":1}]},
                      results:[1,2,3]);
    }

    #[test]
    fn basic2() {
        verify_json!(path:"$.foo.bar", json:{"foo":{"bar":[1,2,3]}}, results:[[1,2,3]]);
    }

    #[test]
    fn basic3() {
        verify_json!(path:"$foo", json:{"foo":[1,2,3]}, results:[[1,2,3]]);
    }

    #[test]
    fn test_expend_all() {
        setup();
        verify_json!(path:"$.foo.*.val",
                           json:{"foo":{"bar1":{"val":[1,2,3]}, "bar2":{"val":[1,2,3]}}},
                           results:[[1,2,3], [1,2,3]]);
    }

    #[test]
    fn test_full_scan() {
        setup();
        verify_json!(path:"$..val",
                           json:{"foo":{"bar1":{"val":[1,2,3]}, "bar2":{"val":[1,2,3]}}, "val":[1,2,3]},
                           results:[[1,2,3], [1,2,3], [1,2,3]]);
    }

    #[test]
    fn test_with_path() {
        setup();
        verify_json_path!(path:"$.foo", json:{"foo":[1,2,3]}, results:[[foo]]);
    }

    #[test]
    fn test_expend_all_with_path() {
        setup();
        verify_json_path!(path:"$.foo.*.val",
                           json:{"foo":{"bar1":{"val":[1,2,3]}, "bar2":{"val":[1,2,3]}}},
                           results:[[foo, bar1, val], [foo, bar2, val]]);
    }

    #[test]
    fn test_expend_all_with_array_path() {
        setup();
        verify_json_path!(path:"$.foo.*.val",
                           json:{"foo":[
                                 {"val":[1,2,3]},
                                 {"val":[1,2,3]}
                             ]
                           },
                           results:[[foo, 0, val], [foo, 1, val]]);
    }

    #[test]
    fn test_query_inside_object_values_indicates_array_path() {
        setup();
        verify_json_path!(path:"$.root[?(@.value > 2)]",
                           json:{
                            "root": {
                              "1": {
                                "value": 1
                              },
                              "2": {
                                "value": 2
                              },
                              "3": {
                                "value": 3
                              },
                              "4": {
                                "value": 4
                              },
                              "5": {
                                "value": 5
                              }
                            }
                          },
                           results:[[root, 3], [root, 4], [root, 5]]);
    }

    #[test]
    fn test_backslash_escape_detailed() {
        setup();
        verify_json!(path:r#"$["\\"]"#, json:{"\\": 1, "\\\\": 2}, results:[1]);
        verify_json!(path:r#"$["\\\\"]"#, json:{"\\": 1, "\\\\": 2}, results:[2]);
        verify_json!(path:r#"$["\\\\\\"]"#, json:{"\\": 1, "\\\\": 2, "\\\\\\": 3}, results:[3]);
        verify_json!(path:r#"$["\\\\\\\\"]"#, json:{"\\": 1, "\\\\": 2, "\\\\\\": 3, "\\\\\\\\": 4}, results:[4]);
    }

    #[test]
    fn test_quote_escape() {
        setup();
        verify_json!(path:r#"$["\""]"#, json:{"\"": 1}, results:[1]);
        verify_json!(path:r#"$["'"]"#, json:{"'": 1}, results:[1]);
        verify_json!(path:r#"$['\'']"#, json:{"'": 1}, results:[1]);
    }

    #[test]
    fn test_tab_escape() {
        setup();
        verify_json!(path:"$[\"\t\"]", json:{"\t": 1}, results:[1]);
    }

    #[test]
    fn test_newline_escape() {
        setup();
        verify_json!(path:"$[\"\n\"]", json:{"\n": 1}, results:[1]);
    }

    #[test]
    fn test_mixed_escapes() {
        setup();
        verify_json!(path:r#"$["\\\""]"#, json:{"\\\"": 1}, results:[1]);
        verify_json!(path:r#"$["a\\b"]"#, json:{"a\\b": 1}, results:[1]);
    }

    #[test]
    fn test_path_calculation_with_escapes() {
        setup();
        use crate::calc_once_paths;
        use crate::compile;
        let test_json = json!({"\\": 1, "\\\\": 2});
        let query1 = compile(r#"$["\\"]"#).unwrap();
        let paths1 = calc_once_paths(query1, &test_json);
        assert_eq!(paths1.len(), 1);
        assert_eq!(paths1[0], vec!["\\".to_string()]);
        let query2 = compile(r#"$["\\\\"]"#).unwrap();
        let paths2 = calc_once_paths(query2, &test_json);
        assert_eq!(paths2.len(), 1);
        assert_eq!(paths2[0], vec!["\\\\".to_string()]);
    }
}
