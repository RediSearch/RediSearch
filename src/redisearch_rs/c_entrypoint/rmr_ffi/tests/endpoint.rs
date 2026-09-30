/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Provides the `RedisModule_Alloc` and `RedisModule_Free` the endpoint strings use.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

use std::ffi::{CStr, CString, c_char};
use std::mem::MaybeUninit;
use std::ptr;

use ffi::{MREndpoint, REDIS_ERR, REDIS_OK};
use rmr_ffi::endpoint::{
    Address, MREndpoint_Copy, MREndpoint_Equal, MREndpoint_Free, MREndpoint_Parse, parse_address,
};

fn address<'a>(password: Option<&'a str>, host: &'a str, port: u16) -> Option<Address<'a>> {
    Some(Address {
        password: password.map(str::as_bytes),
        host: host.as_bytes(),
        port,
    })
}

#[test]
fn parses_hosts_passwords_and_ipv6() {
    let cases = [
        ("localhost:6379", address(None, "localhost", 6379)),
        ("::0:6379", address(None, "::0", 6379)),
        ("[fe80::1]:6380", address(None, "fe80::1", 6380)),
        (
            "pass@[fe80::1]:6380",
            address(Some("pass"), "fe80::1", 6380),
        ),
        // The password ends at the first `@`.
        ("a@b@c:1", address(Some("a"), "b@c", 1)),
        ("@host:1", address(Some(""), "host", 1)),
        ("[]:80", address(None, "", 80)),
    ];
    for (input, expected) in cases {
        assert_eq!(parse_address(input.as_bytes()), expected, "{input}");
    }
}

#[test]
fn rejects_malformed_addresses() {
    for input in [
        "localhost",
        "[fe80::1]",
        "pass@[fe80::1]",
        "[fe80::1:6380",
        "[:6379",
        "localhost:",
        ":-1",
        "localhost:-1",
        "localhost:0",
        "localhost:65536",
        "localhost:655350",
        "localhost:99999999999999999999999",
        // Beyond `int`, where `atoi` may wrap these back to 6379.
        "localhost:4294973675",
        "localhost:-4294960917",
    ] {
        assert_eq!(parse_address(input.as_bytes()), None, "{input}");
    }
}

/// The port is read like C's `atoi`: leading whitespace and a sign are skipped,
/// and it ends at the first non-digit.
#[test]
fn port_is_read_like_atoi() {
    for (input, port) in [
        ("h: 6379", 6379),
        ("h:\x0b6379", 6379),
        ("h:+6379", 6379),
        ("h:6379abc", 6379),
        ("h:65535", 65535),
    ] {
        let parsed = parse_address(input.as_bytes()).map(|address| address.port);
        assert_eq!(parsed, Some(port), "{input:?}");
    }
}

fn parse(addr: &str) -> (i32, MREndpoint) {
    let addr = CString::new(addr).unwrap();
    let mut ep = MaybeUninit::<MREndpoint>::uninit();
    // SAFETY: `addr` is nul-terminated and `ep` is valid for writes.
    let rc = unsafe { MREndpoint_Parse(addr.as_ptr(), ep.as_mut_ptr()) };
    // SAFETY: `MREndpoint_Parse` always initializes `ep`.
    (rc, unsafe { ep.assume_init() })
}

fn string(ptr: *const c_char) -> Option<String> {
    // SAFETY: the endpoint's strings are null or nul-terminated.
    (!ptr.is_null()).then(|| unsafe { CStr::from_ptr(ptr) }.to_str().unwrap().to_owned())
}

fn c_string(string: &'static CStr) -> *mut c_char {
    string.as_ptr().cast_mut()
}

/// Returns whether `MREndpoint_Equal` holds both ways round, and asserts that the
/// two agree.
fn equal(a: &MREndpoint, b: &MREndpoint) -> bool {
    // SAFETY: both endpoints are valid, with nul-terminated or null strings.
    let (ab, ba) = unsafe { (MREndpoint_Equal(a, b), MREndpoint_Equal(b, a)) };
    assert_eq!(ab, ba);
    ab
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn parses_into_an_endpoint_that_owns_its_strings() {
    let (rc, mut ep) = parse("pass@[fe80::1]:6380");
    assert_eq!(rc, REDIS_OK as i32);
    assert_eq!(string(ep.host).as_deref(), Some("fe80::1"));
    assert_eq!(string(ep.password).as_deref(), Some("pass"));
    assert_eq!(ep.port, 6380);
    assert!(ep.unixSock.is_null() && !ep.isTls);

    // SAFETY: `ep` owns its `RedisModule_Alloc`-allocated strings.
    unsafe { MREndpoint_Free(&mut ep) };
    assert!(ep.host.is_null() && ep.password.is_null() && ep.unixSock.is_null());
}

#[test]
fn failed_parse_zeroes_the_endpoint() {
    let (rc, ep) = parse("pass@localhost:655350");
    assert_eq!(rc, REDIS_ERR);
    assert!(ep.host.is_null() && ep.password.is_null() && ep.unixSock.is_null());
    assert!(ep.port == 0 && !ep.isTls);
}

#[test]
#[cfg_attr(
    miri,
    ignore = "extern static `RedisModule_Alloc` is not supported by Miri"
)]
fn copy_owns_copies_of_every_string() {
    let src = MREndpoint {
        host: c_string(c"host"),
        port: 6379,
        isTls: true,
        unixSock: c_string(c"/tmp/redis.sock"),
        password: c_string(c"pass"),
    };
    let mut copy = MaybeUninit::<MREndpoint>::uninit();
    // SAFETY: `src` is valid, and `copy` is valid for writes.
    unsafe { MREndpoint_Copy(copy.as_mut_ptr(), &src) };
    // SAFETY: `MREndpoint_Copy` initializes `copy`.
    let mut copy = unsafe { copy.assume_init() };

    assert!(equal(&src, &copy));
    for (original, copied) in [
        (src.host, copy.host),
        (src.unixSock, copy.unixSock),
        (src.password, copy.password),
    ] {
        assert_ne!(original, copied);
    }

    // SAFETY: `copy` owns its `RedisModule_Alloc`-allocated strings.
    unsafe { MREndpoint_Free(&mut copy) };
    assert!(copy.host.is_null() && copy.password.is_null() && copy.unixSock.is_null());
}

#[test]
fn endpoints_are_equal_only_if_every_field_is() {
    let base = MREndpoint {
        host: c_string(c"host"),
        port: 6379,
        isTls: false,
        unixSock: c_string(c"/tmp/redis.sock"),
        password: c_string(c"pass"),
    };
    let (host, unix_sock, password) = (
        c"host".to_owned(),
        c"/tmp/redis.sock".to_owned(),
        c"pass".to_owned(),
    );
    let same = MREndpoint {
        host: host.as_ptr().cast_mut(),
        unixSock: unix_sock.as_ptr().cast_mut(),
        password: password.as_ptr().cast_mut(),
        ..base
    };
    assert!(equal(&base, &same), "strings are compared by content");

    let no_strings = || MREndpoint {
        host: ptr::null_mut(),
        unixSock: ptr::null_mut(),
        password: ptr::null_mut(),
        ..base
    };
    assert!(equal(&no_strings(), &no_strings()));

    for other in [
        MREndpoint { port: 6380, ..base },
        MREndpoint {
            isTls: true,
            ..base
        },
        MREndpoint {
            host: c_string(c"other"),
            ..base
        },
        MREndpoint {
            host: ptr::null_mut(),
            ..base
        },
        MREndpoint {
            unixSock: c_string(c"/tmp/other.sock"),
            ..base
        },
        MREndpoint {
            unixSock: ptr::null_mut(),
            ..base
        },
        MREndpoint {
            password: c_string(c"other"),
            ..base
        },
        MREndpoint {
            password: ptr::null_mut(),
            ..base
        },
    ] {
        assert!(!equal(&base, &other), "{other:?}");
    }
}

#[test]
fn null_endpoints_equal_only_null() {
    let ep = MREndpoint {
        host: c_string(c"x"),
        port: 1,
        isTls: false,
        unixSock: ptr::null_mut(),
        password: ptr::null_mut(),
    };
    // SAFETY: both are null.
    assert!(unsafe { MREndpoint_Equal(ptr::null(), ptr::null()) });
    // SAFETY: `ep` is valid; the other is null.
    assert!(!unsafe { MREndpoint_Equal(&ep, ptr::null()) });
    // SAFETY: as above.
    assert!(!unsafe { MREndpoint_Equal(ptr::null(), &ep) });
}
