/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of (a) the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

// Custom serde_json formatter supporting ReJSON formatting options.
// Based on serde_json::ser::PrettyFormatter
/*
Permission is hereby granted, free of charge, to any
person obtaining a copy of this software and associated
documentation files (the "Software"), to deal in the
Software without restriction, including without
limitation the rights to use, copy, modify, merge,
publish, distribute, sublicense, and/or sell copies of
the Software, and to permit persons to whom the Software
is furnished to do so, subject to the following
conditions:

The above copyright notice and this permission notice
shall be included in all copies or substantial portions
of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF
ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT
SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR
IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
DEALINGS IN THE SOFTWARE.
*/

use serde_json::ser::Formatter;
use std::io;

pub use crate::redisjson::ReplyFormat;

pub struct ReplyFormatOptions<'a> {
    pub format: ReplyFormat,
    pub indent: Option<&'a str>,
    pub space: Option<&'a str>,
    pub newline: Option<&'a str>,
    pub resp3: bool,
}

impl ReplyFormatOptions<'_> {
    /// Creates a new FormatOptions
    pub fn new(resp3: bool, format: ReplyFormat) -> Self {
        Self {
            format,
            indent: None,
            space: None,
            newline: None,
            resp3,
        }
    }

    /// Returns true if the format is RESP3 and the format is not STRING/STRINGS format
    /// STRING/STRINGS format (depending on the command) is fully backward compatible with RESP2
    pub fn is_resp3_reply(&self) -> bool {
        self.resp3 && self.format != ReplyFormat::STRING && self.format != ReplyFormat::STRINGS
    }

    /// Checks if the JSON formatting options are the default ones with no overrides
    pub fn no_formatting(&self) -> bool {
        self.indent.is_none() && self.space.is_none() && self.newline.is_none()
    }
}

impl Default for ReplyFormatOptions<'_> {
    /// Creates a new FormatOptions with the default values matching RESP2
    fn default() -> Self {
        Self {
            format: ReplyFormat::STRING,
            indent: None,
            space: None,
            newline: None,
            resp3: false,
        }
    }
}

pub struct RedisJsonFormatter<'a> {
    current_indent: usize,
    has_value: bool,
    indent: Option<&'a str>,
    space: Option<&'a str>,
    newline: Option<&'a str>,
}

impl<'a> RedisJsonFormatter<'a> {
    pub const fn new(format: &'a ReplyFormatOptions) -> Self {
        RedisJsonFormatter {
            current_indent: 0,
            has_value: false,
            indent: format.indent,
            space: format.space,
            newline: format.newline,
        }
    }

    fn new_line<W>(&self, wr: &mut W) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        // Write new line if defined
        if let Some(n) = self.newline {
            wr.write_all(n.as_bytes())?;
        }

        // Ident the next line if defined
        if let Some(s) = self.indent {
            let bytes = s.as_bytes();
            for _ in 0..self.current_indent {
                wr.write_all(bytes)?;
            }
        }

        Ok(())
    }
}

impl Formatter for RedisJsonFormatter<'_> {
    fn begin_array<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        self.current_indent += 1;
        self.has_value = false;
        writer.write_all(b"[")
    }

    fn end_array<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        self.current_indent -= 1;

        if self.has_value {
            self.new_line(writer)?;
        }

        writer.write_all(b"]")
    }

    fn begin_array_value<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        if !first {
            writer.write_all(b",")?;
        }
        self.new_line(writer)
    }

    fn end_array_value<W>(&mut self, _writer: &mut W) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        self.has_value = true;
        Ok(())
    }

    fn begin_object<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        self.current_indent += 1;
        self.has_value = false;
        writer.write_all(b"{")
    }

    fn end_object<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        self.current_indent -= 1;

        if self.has_value {
            self.new_line(writer)?;
        }

        writer.write_all(b"}")
    }

    fn begin_object_key<W>(&mut self, writer: &mut W, first: bool) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        if !first {
            writer.write_all(b",")?;
        }
        self.new_line(writer)
    }

    fn begin_object_value<W>(&mut self, writer: &mut W) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        writer.write_all(b":")?;
        self.space
            .map_or(Ok(()), |s| writer.write_all(s.as_bytes()))
    }

    fn end_object_value<W>(&mut self, _writer: &mut W) -> io::Result<()>
    where
        W: io::Write + ?Sized,
    {
        self.has_value = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_default_formatter() {
        let mut formatter = RedisJsonFormatter::new(&ReplyFormatOptions {
            format: ReplyFormat::STRING,
            indent: None,
            space: None,
            newline: None,
            resp3: false,
        });
        let mut writer = vec![];

        assert!(matches!(formatter.begin_array(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[");

        assert!(matches!(formatter.begin_object(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{");

        assert!(matches!(
            formatter.begin_object_key(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[{");

        assert!(matches!(
            formatter.begin_object_key(&mut writer, false),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[{,");

        assert!(matches!(formatter.begin_object_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:");

        assert!(matches!(formatter.end_object_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:");

        assert!(matches!(formatter.end_object(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:}");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[{,:}");

        assert!(matches!(formatter.end_array_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:}");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, false),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[{,:},");

        assert!(matches!(formatter.end_array(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:},]");
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_ident_formatter() {
        let mut formatter = RedisJsonFormatter::new(&ReplyFormatOptions {
            format: ReplyFormat::STRING,
            indent: Some("_"),
            space: None,
            newline: None,
            resp3: false,
        });
        let mut writer = vec![];

        assert!(matches!(formatter.begin_array(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[_");

        assert!(matches!(formatter.begin_object(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[_{");

        assert!(matches!(
            formatter.begin_object_key(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[_{__");

        assert!(matches!(
            formatter.begin_object_key(&mut writer, false),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[_{__,__");

        assert!(matches!(formatter.begin_object_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[_{__,__:");

        assert!(matches!(formatter.end_object_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[_{__,__:");

        assert!(matches!(formatter.end_object(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[_{__,__:_}");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[_{__,__:_}_");

        assert!(matches!(formatter.end_array_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[_{__,__:_}_");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, false),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[_{__,__:_}_,_");

        assert!(matches!(formatter.end_array(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[_{__,__:_}_,_]");
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_space_formatter() {
        let mut formatter = RedisJsonFormatter::new(&ReplyFormatOptions {
            format: ReplyFormat::STRING,
            indent: None,
            space: Some("s"),
            newline: None,
            resp3: false,
        });
        let mut writer = vec![];

        assert!(matches!(formatter.begin_array(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[");

        assert!(matches!(formatter.begin_object(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{");

        assert!(matches!(
            formatter.begin_object_key(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[{");

        assert!(matches!(
            formatter.begin_object_key(&mut writer, false),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[{,");

        assert!(matches!(formatter.begin_object_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:s");

        assert!(matches!(formatter.end_object_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:s");

        assert!(matches!(formatter.end_object(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:s}");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[{,:s}");

        assert!(matches!(formatter.end_array_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:s}");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, false),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[{,:s},");

        assert!(matches!(formatter.end_array(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[{,:s},]");
    }

    #[test]
    #[allow(clippy::cognitive_complexity)]
    fn test_new_line_formatter() {
        let mut formatter = RedisJsonFormatter::new(&ReplyFormatOptions {
            format: ReplyFormat::STRING,
            indent: None,
            space: None,
            newline: Some("n"),
            resp3: false,
        });
        let mut writer = vec![];

        assert!(matches!(formatter.begin_array(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[n");

        assert!(matches!(formatter.begin_object(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[n{");

        assert!(matches!(
            formatter.begin_object_key(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[n{n");

        assert!(matches!(
            formatter.begin_object_key(&mut writer, false),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[n{n,n");

        assert!(matches!(formatter.begin_object_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[n{n,n:");

        assert!(matches!(formatter.end_object_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[n{n,n:");

        assert!(matches!(formatter.end_object(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[n{n,n:n}");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, true),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[n{n,n:n}n");

        assert!(matches!(formatter.end_array_value(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[n{n,n:n}n");

        assert!(matches!(
            formatter.begin_array_value(&mut writer, false),
            Ok(())
        ));
        assert_eq!(&writer[0..], b"[n{n,n:n}n,n");

        assert!(matches!(formatter.end_array(&mut writer), Ok(())));
        assert_eq!(&writer[0..], b"[n{n,n:n}n,nn]");
    }
}
