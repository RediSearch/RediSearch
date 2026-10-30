/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Calendar dates as day numbers, which plot on a plain integer axis.

use std::fmt;
use std::str::FromStr;

/// A proleptic Gregorian date, as the number of days since 1970-01-01.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Date(pub i64);

impl Date {
    pub fn from_ymd(year: i64, month: u32, day: u32) -> Self {
        // Howard Hinnant's `days_from_civil`, counting years from March so
        // that leap days end them.
        let year = if month <= 2 { year - 1 } else { year };
        let era = year.div_euclid(400);
        let year_of_era = year - era * 400;
        let month_from_march = (i64::from(month) + 9) % 12;
        let day_of_year = (153 * month_from_march + 2) / 5 + i64::from(day) - 1;
        let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
        Self(era * 146_097 + day_of_era - 719_468)
    }

    /// The year, month and day, inverting [`Date::from_ymd`].
    pub fn ymd(self) -> (i64, u32, u32) {
        let days = self.0 + 719_468;
        let era = days.div_euclid(146_097);
        let day_of_era = days - era * 146_097;
        let year_of_era =
            (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
        let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
        let month_from_march = (5 * day_of_year + 2) / 153;
        let day = (day_of_year - (153 * month_from_march + 2) / 5 + 1) as u32;
        let month = if month_from_march < 10 {
            month_from_march + 3
        } else {
            month_from_march - 9
        } as u32;
        let year = era * 400 + year_of_era + i64::from(month <= 2);
        (year, month, day)
    }

    /// The first day of every `step`-th month after `self`, up to `end`.
    pub fn month_starts(self, end: Self, step: u32) -> Vec<Self> {
        let (mut year, mut month, day) = self.ymd();
        if day != 1 {
            (year, month) = next_month(year, month, 1);
        }
        let mut starts = Vec::new();
        loop {
            let start = Self::from_ymd(year, month, 1);
            if start > end {
                return starts;
            }
            starts.push(start);
            (year, month) = next_month(year, month, step);
        }
    }
}

const fn next_month(year: i64, month: u32, step: u32) -> (i64, u32) {
    let index = month - 1 + step;
    (year + (index / 12) as i64, index % 12 + 1)
}

impl FromStr for Date {
    type Err = String;

    /// Parses `YYYY-MM-DD`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let error = || format!("malformed date {s:?}");
        let mut parts = s.splitn(3, '-');
        let mut next = || parts.next().ok_or_else(error);
        let year: i64 = next()?.parse().map_err(|_| error())?;
        let month: u32 = next()?.parse().map_err(|_| error())?;
        let day: u32 = next()?.parse().map_err(|_| error())?;
        if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
            return Err(error());
        }
        Ok(Self::from_ymd(year, month, day))
    }
}

impl fmt::Display for Date {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let (year, month, day) = self.ymd();
        write!(f, "{year:04}-{month:02}-{day:02}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_day_numbers() {
        assert_eq!(Date::from_ymd(1970, 1, 1), Date(0));
        assert_eq!(Date::from_ymd(2000, 3, 1), Date(11_017));
        assert_eq!(Date::from_ymd(1969, 12, 31), Date(-1));
    }

    #[test]
    #[cfg_attr(miri, ignore = "walks ~37,000 days, too slow under Miri")]
    fn round_trips_through_leap_years() {
        for days in Date::from_ymd(1999, 1, 1).0..Date::from_ymd(2101, 1, 1).0 {
            let date = Date(days);
            let (y, m, d) = date.ymd();
            assert_eq!(Date::from_ymd(y, m, d), date);
            assert_eq!(date.to_string().parse::<Date>(), Ok(date));
        }
        assert_eq!("2024-02-29".parse::<Date>().unwrap().ymd(), (2024, 2, 29));
    }

    #[test]
    fn rejects_malformed_dates() {
        for s in ["", "2024", "2024-13-01", "2024-01-00", "2024-01-xx"] {
            assert!(s.parse::<Date>().is_err(), "{s}");
        }
    }

    #[test]
    fn month_starts_skip_a_partial_first_month() {
        let from = Date::from_ymd(2025, 11, 15);
        let to = Date::from_ymd(2026, 3, 1);
        let starts: Vec<String> = from
            .month_starts(to, 2)
            .iter()
            .map(Date::to_string)
            .collect();
        assert_eq!(starts, ["2025-12-01", "2026-02-01"]);
        assert_eq!(
            Date::from_ymd(2025, 1, 1).month_starts(to, 12),
            [Date::from_ymd(2025, 1, 1), Date::from_ymd(2026, 1, 1)]
        );
    }
}
