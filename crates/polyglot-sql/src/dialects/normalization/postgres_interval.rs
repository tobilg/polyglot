use crate::expressions::{DataType, Expression, IntervalUnit, IntervalUnitSpec, Literal};

pub(super) const MICROS_PER_SECOND: i128 = 1_000_000;
pub(super) const MICROS_PER_MINUTE: i128 = 60 * MICROS_PER_SECOND;
pub(super) const MICROS_PER_HOUR: i128 = 60 * MICROS_PER_MINUTE;
pub(super) const MICROS_PER_DAY: i128 = 24 * MICROS_PER_HOUR;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct PostgresInterval {
    pub(super) months: i128,
    pub(super) days: i128,
    pub(super) micros: i128,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct SimpleInterval {
    pub(super) amount: i128,
    pub(super) unit: IntervalUnit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ParsedInterval {
    pub(super) value: PostgresInterval,
    pub(super) simple: Option<SimpleInterval>,
    pub(super) has_date_fields: bool,
    pub(super) has_time_fields: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum DecomposeOutcome {
    NotLiteral,
    Invalid,
    Parsed(ParsedInterval),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Unit {
    Millennium,
    Century,
    Decade,
    Year,
    Quarter,
    Month,
    Week,
    Day,
    Hour,
    Minute,
    Second,
    Millisecond,
    Microsecond,
    Nanosecond,
}

impl Unit {
    fn interval_unit(self) -> Option<IntervalUnit> {
        match self {
            Self::Year => Some(IntervalUnit::Year),
            Self::Quarter => Some(IntervalUnit::Quarter),
            Self::Month => Some(IntervalUnit::Month),
            Self::Week => Some(IntervalUnit::Week),
            Self::Day => Some(IntervalUnit::Day),
            Self::Hour => Some(IntervalUnit::Hour),
            Self::Minute => Some(IntervalUnit::Minute),
            Self::Second => Some(IntervalUnit::Second),
            Self::Millisecond => Some(IntervalUnit::Millisecond),
            Self::Microsecond => Some(IntervalUnit::Microsecond),
            Self::Nanosecond => Some(IntervalUnit::Nanosecond),
            Self::Millennium | Self::Century | Self::Decade => None,
        }
    }

    fn is_date(self) -> bool {
        matches!(
            self,
            Self::Millennium
                | Self::Century
                | Self::Decade
                | Self::Year
                | Self::Quarter
                | Self::Month
                | Self::Week
                | Self::Day
        )
    }
}

#[derive(Debug, Clone, Copy)]
struct Qualifier {
    start: Unit,
    end: Unit,
    second_precision: Option<u32>,
}

#[derive(Debug, Clone, Copy)]
struct Decimal {
    coefficient: i128,
    scale: u32,
}

#[derive(Debug, Default)]
struct Accumulator {
    value: PostgresInterval,
    has_date_fields: bool,
    has_time_fields: bool,
}

pub(super) fn decompose(expression: &Expression) -> DecomposeOutcome {
    let Some((literal, qualifier)) = interval_literal(expression) else {
        return if is_interval_expression(expression) {
            DecomposeOutcome::NotLiteral
        } else {
            DecomposeOutcome::Invalid
        };
    };

    if is_colon_parameter(literal) {
        return DecomposeOutcome::NotLiteral;
    }

    if literal.eq_ignore_ascii_case("infinity") || literal.eq_ignore_ascii_case("-infinity") {
        return DecomposeOutcome::Invalid;
    }

    let simple = simple_interval(literal, qualifier);
    match parse(literal, qualifier) {
        Some(acc) => DecomposeOutcome::Parsed(ParsedInterval {
            value: acc.value,
            simple,
            has_date_fields: acc.has_date_fields,
            has_time_fields: acc.has_time_fields,
        }),
        None => DecomposeOutcome::Invalid,
    }
}

fn is_colon_parameter(value: &str) -> bool {
    let Some(name) = value.strip_prefix(':') else {
        return false;
    };
    !name.is_empty()
        && name
            .chars()
            .all(|character| character.is_ascii_alphanumeric() || character == '_')
}

fn is_interval_expression(expression: &Expression) -> bool {
    match expression {
        Expression::Interval(_) => true,
        Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c) => {
            matches!(c.to, DataType::Interval { .. })
                || matches!(&c.to, DataType::Custom { name } if name.eq_ignore_ascii_case("INTERVAL"))
        }
        _ => false,
    }
}

fn interval_literal(expression: &Expression) -> Option<(&str, Option<Qualifier>)> {
    match expression {
        Expression::Interval(interval) => {
            let literal = string_literal(interval.this.as_ref()?)?;
            let qualifier = interval.unit.as_ref().and_then(qualifier_from_spec);
            Some((literal, qualifier))
        }
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast)
            if is_interval_data_type(&cast.to) =>
        {
            let literal = string_literal(&cast.this)?;
            let qualifier = qualifier_from_data_type(&cast.to);
            Some((literal, qualifier))
        }
        _ => None,
    }
}

fn is_interval_data_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Interval { .. })
        || matches!(data_type, DataType::Custom { name } if name.eq_ignore_ascii_case("INTERVAL"))
}

fn string_literal(expression: &Expression) -> Option<&str> {
    match expression {
        Expression::Literal(literal) => match literal.as_ref() {
            Literal::String(value) | Literal::Number(value) => Some(value),
            _ => None,
        },
        _ => None,
    }
}

fn qualifier_from_data_type(data_type: &DataType) -> Option<Qualifier> {
    let DataType::Interval { unit, to } = data_type else {
        return None;
    };
    let start = parse_unit(unit.as_deref()?)?;
    let end = to.as_deref().and_then(parse_unit).unwrap_or(start);
    Some(Qualifier {
        start,
        end,
        second_precision: None,
    })
}

fn qualifier_from_spec(spec: &IntervalUnitSpec) -> Option<Qualifier> {
    match spec {
        IntervalUnitSpec::Simple { unit, .. } => {
            let unit = unit_from_interval(*unit);
            Some(Qualifier {
                start: unit,
                end: unit,
                second_precision: None,
            })
        }
        IntervalUnitSpec::Span(span) => Some(Qualifier {
            start: unit_from_interval(span.this),
            end: unit_from_interval(span.expression),
            second_precision: None,
        }),
        IntervalUnitSpec::Expr(expression) => {
            let (unit, precision) = unit_from_expression(expression)?;
            Some(Qualifier {
                start: unit,
                end: unit,
                second_precision: (unit == Unit::Second).then_some(precision).flatten(),
            })
        }
        IntervalUnitSpec::ExprSpan(span) => {
            let (start, _) = unit_from_expression(&span.this)?;
            let (end, precision) = unit_from_expression(&span.expression)?;
            Some(Qualifier {
                start,
                end,
                second_precision: (end == Unit::Second).then_some(precision).flatten(),
            })
        }
    }
}

fn unit_from_interval(unit: IntervalUnit) -> Unit {
    match unit {
        IntervalUnit::Year => Unit::Year,
        IntervalUnit::Quarter => Unit::Quarter,
        IntervalUnit::Month => Unit::Month,
        IntervalUnit::Week => Unit::Week,
        IntervalUnit::Day => Unit::Day,
        IntervalUnit::Hour => Unit::Hour,
        IntervalUnit::Minute => Unit::Minute,
        IntervalUnit::Second => Unit::Second,
        IntervalUnit::Millisecond => Unit::Millisecond,
        IntervalUnit::Microsecond => Unit::Microsecond,
        IntervalUnit::Nanosecond => Unit::Nanosecond,
    }
}

fn unit_from_expression(expression: &Expression) -> Option<(Unit, Option<u32>)> {
    let unary = |unit, value: &Expression| Some((unit, precision(value)));
    match expression {
        Expression::Year(value) => unary(Unit::Year, &value.this),
        Expression::Quarter(value) => unary(Unit::Quarter, &value.this),
        Expression::Month(value) => unary(Unit::Month, &value.this),
        Expression::Day(value) => unary(Unit::Day, &value.this),
        Expression::Hour(value) => unary(Unit::Hour, &value.this),
        Expression::Minute(value) => unary(Unit::Minute, &value.this),
        Expression::Second(value) => unary(Unit::Second, &value.this),
        Expression::Anonymous(value) => {
            let name = expression_name(&value.this)?;
            let unit = parse_unit(name)?;
            Some((unit, value.expressions.first().and_then(precision)))
        }
        Expression::Identifier(identifier) => Some((parse_unit(&identifier.name)?, None)),
        Expression::Var(var) => Some((parse_unit(&var.this)?, None)),
        Expression::Column(column) if column.table.is_none() => {
            Some((parse_unit(&column.name.name)?, None))
        }
        _ => None,
    }
}

fn expression_name(expression: &Expression) -> Option<&str> {
    match expression {
        Expression::Identifier(identifier) => Some(&identifier.name),
        Expression::Var(var) => Some(&var.this),
        Expression::Column(column) if column.table.is_none() => Some(&column.name.name),
        _ => None,
    }
}

fn precision(expression: &Expression) -> Option<u32> {
    string_literal(expression)?.parse().ok()
}

fn parse_unit(unit: &str) -> Option<Unit> {
    match unit.trim().to_ascii_lowercase().as_str() {
        "millennium" | "millennia" | "mil" | "mils" => Some(Unit::Millennium),
        "century" | "centuries" | "c" | "cent" | "cents" => Some(Unit::Century),
        "decade" | "decades" | "dec" | "decs" => Some(Unit::Decade),
        "year" | "years" | "y" | "yr" | "yrs" | "yy" | "yyyy" => Some(Unit::Year),
        "quarter" | "quarters" | "q" | "qtr" | "qtrs" | "qq" => Some(Unit::Quarter),
        "month" | "months" | "mon" | "mons" | "mm" => Some(Unit::Month),
        "week" | "weeks" | "w" | "wk" | "wks" | "ww" => Some(Unit::Week),
        "day" | "days" | "d" | "dd" => Some(Unit::Day),
        "hour" | "hours" | "h" | "hr" | "hrs" | "hh" => Some(Unit::Hour),
        "minute" | "minutes" | "m" | "min" | "mins" | "mi" | "n" => Some(Unit::Minute),
        "second" | "seconds" | "s" | "sec" | "secs" | "ss" => Some(Unit::Second),
        "millisecond" | "milliseconds" | "ms" | "msec" | "msecs" => Some(Unit::Millisecond),
        "microsecond" | "microseconds" | "us" | "usec" | "usecs" => Some(Unit::Microsecond),
        "nanosecond" | "nanoseconds" | "ns" | "nsec" | "nsecs" => Some(Unit::Nanosecond),
        _ => None,
    }
}

fn simple_interval(literal: &str, qualifier: Option<Qualifier>) -> Option<SimpleInterval> {
    let value = literal.trim();
    if value.starts_with('@') || value.to_ascii_lowercase().ends_with("ago") {
        return None;
    }
    let parts: Vec<_> = value.split_whitespace().collect();
    let (amount, unit) = match parts.as_slice() {
        [amount, unit] => (parse_integer(amount)?, parse_unit(unit)?),
        [amount] => {
            let qualifier = qualifier?;
            if qualifier.start != qualifier.end {
                return None;
            }
            (parse_integer(amount)?, qualifier.start)
        }
        _ => return None,
    };
    Some(SimpleInterval {
        amount,
        unit: unit.interval_unit()?,
    })
}

fn parse_integer(value: &str) -> Option<i128> {
    if value.contains(['.', 'e', 'E']) {
        return None;
    }
    value.parse().ok()
}

fn parse(literal: &str, qualifier: Option<Qualifier>) -> Option<Accumulator> {
    let mut value = literal.trim();
    let mut negate = false;
    if let Some(rest) = value.strip_prefix('@') {
        value = rest.trim_start();
    }
    if value.len() >= 3 && value[value.len() - 3..].eq_ignore_ascii_case("ago") {
        value = value[..value.len() - 3].trim_end();
        negate = true;
    }

    let mut accumulator = if starts_iso_interval(value) {
        parse_iso(value)?
    } else {
        parse_traditional(value, qualifier)?
    };
    apply_qualifier(&mut accumulator.value, qualifier)?;
    if negate {
        accumulator.value.months = accumulator.value.months.checked_neg()?;
        accumulator.value.days = accumulator.value.days.checked_neg()?;
        accumulator.value.micros = accumulator.value.micros.checked_neg()?;
    }
    Some(accumulator)
}

fn starts_iso_interval(value: &str) -> bool {
    value.trim_start_matches(['+', '-']).starts_with(['P', 'p'])
}

fn parse_traditional(value: &str, qualifier: Option<Qualifier>) -> Option<Accumulator> {
    let tokens: Vec<_> = value.split_whitespace().collect();
    if tokens.is_empty() {
        return None;
    }

    let mut accumulator = Accumulator::default();
    let mut index = 0;
    while index < tokens.len() {
        let token = tokens[index];
        if token.contains(':') {
            let micros = parse_clock(token)?;
            accumulator.value.micros = accumulator.value.micros.checked_add(micros)?;
            accumulator.has_time_fields = true;
            index += 1;
            continue;
        }
        if let Some((years, months)) = parse_year_month(token) {
            accumulator.value.months = accumulator
                .value
                .months
                .checked_add(years.checked_mul(12)?.checked_add(months)?)?;
            accumulator.has_date_fields = true;
            index += 1;
            continue;
        }

        let quantity = Decimal::parse(token)?;
        if let Some(unit_text) = tokens.get(index + 1) {
            if let Some(unit) = parse_unit(unit_text) {
                accumulator.add(quantity, unit)?;
                index += 2;
                continue;
            }
            if unit_text.contains(':') {
                accumulator.add(quantity, Unit::Day)?;
                index += 1;
                continue;
            }
        }

        if tokens.len() == 1 {
            accumulator.add(quantity, qualifier.map_or(Unit::Second, |q| q.start))?;
            index += 1;
            continue;
        }
        return None;
    }
    Some(accumulator)
}

fn parse_year_month(value: &str) -> Option<(i128, i128)> {
    let (sign, value) = if let Some(rest) = value.strip_prefix('-') {
        (-1_i128, rest)
    } else if let Some(rest) = value.strip_prefix('+') {
        (1, rest)
    } else {
        (1, value)
    };
    let (years, months) = value.split_once('-')?;
    if years.is_empty() || months.is_empty() || months.contains('-') {
        return None;
    }
    let years = years.parse::<i128>().ok()?.checked_mul(sign)?;
    let months = months.parse::<i128>().ok()?.checked_mul(sign)?;
    Some((years, months))
}

fn parse_clock(value: &str) -> Option<i128> {
    let (sign, value) = if let Some(rest) = value.strip_prefix('-') {
        (-1_i128, rest)
    } else if let Some(rest) = value.strip_prefix('+') {
        (1, rest)
    } else {
        (1, value)
    };
    let parts: Vec<_> = value.split(':').collect();
    if !(2..=3).contains(&parts.len()) {
        return None;
    }
    let hours = Decimal::parse(parts[0])?;
    let minutes = Decimal::parse(parts[1])?;
    if hours.scale != 0 || minutes.scale != 0 || !(0..60).contains(&minutes.coefficient) {
        return None;
    }
    let seconds = if let Some(seconds) = parts.get(2) {
        let seconds = Decimal::parse(seconds)?;
        if seconds.coefficient < 0 || decimal_at_least(&seconds, 60)? {
            return None;
        }
        seconds
    } else {
        Decimal {
            coefficient: 0,
            scale: 0,
        }
    };
    let micros = hours
        .coefficient
        .checked_mul(MICROS_PER_HOUR)?
        .checked_add(minutes.coefficient.checked_mul(MICROS_PER_MINUTE)?)?
        .checked_add(seconds.scaled_round(MICROS_PER_SECOND, 1)?)?;
    micros.checked_mul(sign)
}

fn decimal_at_least(value: &Decimal, limit: i128) -> Option<bool> {
    Some(value.coefficient >= limit.checked_mul(pow10(value.scale)?)?)
}

fn parse_iso(value: &str) -> Option<Accumulator> {
    let (sign, value) = if let Some(rest) = value.strip_prefix('-') {
        (-1_i128, rest)
    } else if let Some(rest) = value.strip_prefix('+') {
        (1, rest)
    } else {
        (1, value)
    };
    let body = value.strip_prefix(['P', 'p'])?;
    let mut accumulator = if is_iso_alternative(body) {
        parse_iso_alternative(body)?
    } else {
        parse_iso_designator(body)?
    };
    accumulator.value.months = accumulator.value.months.checked_mul(sign)?;
    accumulator.value.days = accumulator.value.days.checked_mul(sign)?;
    accumulator.value.micros = accumulator.value.micros.checked_mul(sign)?;
    Some(accumulator)
}

fn is_iso_alternative(body: &str) -> bool {
    body.split_once(['T', 't'])
        .is_some_and(|(date, time)| date.matches('-').count() == 2 && time.contains(':'))
}

fn parse_iso_alternative(body: &str) -> Option<Accumulator> {
    let (date, time) = body.split_once(['T', 't'])?;
    let date: Vec<_> = date.split('-').collect();
    if date.len() != 3 {
        return None;
    }
    let mut accumulator = Accumulator::default();
    accumulator.add(Decimal::parse(date[0])?, Unit::Year)?;
    accumulator.add(Decimal::parse(date[1])?, Unit::Month)?;
    accumulator.add(Decimal::parse(date[2])?, Unit::Day)?;
    accumulator.value.micros = accumulator.value.micros.checked_add(parse_clock(time)?)?;
    accumulator.has_time_fields = true;
    Some(accumulator)
}

fn parse_iso_designator(body: &str) -> Option<Accumulator> {
    if body.is_empty() {
        return None;
    }
    let mut accumulator = Accumulator::default();
    let mut in_time = false;
    let mut start = 0;
    let chars: Vec<_> = body.char_indices().collect();
    for (position, ch) in chars.iter().copied() {
        if matches!(ch, 'T' | 't') {
            if position != start {
                return None;
            }
            in_time = true;
            start = position + ch.len_utf8();
            continue;
        }
        let unit = match ch {
            'Y' | 'y' => Some(Unit::Year),
            'M' | 'm' if in_time => Some(Unit::Minute),
            'M' | 'm' => Some(Unit::Month),
            'W' | 'w' => Some(Unit::Week),
            'D' | 'd' => Some(Unit::Day),
            'H' | 'h' => Some(Unit::Hour),
            'S' | 's' => Some(Unit::Second),
            _ => None,
        };
        if let Some(unit) = unit {
            if position == start {
                return None;
            }
            accumulator.add(Decimal::parse(&body[start..position])?, unit)?;
            start = position + ch.len_utf8();
        }
    }
    if start != body.len() {
        return None;
    }
    Some(accumulator)
}

impl Accumulator {
    fn add(&mut self, quantity: Decimal, unit: Unit) -> Option<()> {
        if unit.is_date() {
            self.has_date_fields = true;
        } else {
            self.has_time_fields = true;
        }
        match unit {
            Unit::Millennium => self.add_rounded_months(quantity, 12_000),
            Unit::Century => self.add_rounded_months(quantity, 1_200),
            Unit::Decade => self.add_rounded_months(quantity, 120),
            Unit::Year => self.add_rounded_months(quantity, 12),
            Unit::Quarter => self.add_rounded_months(quantity, 3),
            Unit::Month => self.add_months(quantity),
            Unit::Week => self.add_days(quantity, 7),
            Unit::Day => self.add_days(quantity, 1),
            Unit::Hour => self.add_micros(quantity, MICROS_PER_HOUR, 1),
            Unit::Minute => self.add_micros(quantity, MICROS_PER_MINUTE, 1),
            Unit::Second => self.add_micros(quantity, MICROS_PER_SECOND, 1),
            Unit::Millisecond => self.add_micros(quantity, 1_000, 1),
            Unit::Microsecond => self.add_micros(quantity, 1, 1),
            Unit::Nanosecond => self.add_micros(quantity, 1, 1_000),
        }
    }

    fn add_rounded_months(&mut self, quantity: Decimal, multiplier: i128) -> Option<()> {
        self.value.months = self
            .value
            .months
            .checked_add(quantity.scaled_round(multiplier, 1)?)?;
        Some(())
    }

    fn add_months(&mut self, quantity: Decimal) -> Option<()> {
        let denominator = pow10(quantity.scale)?;
        let months = quantity.coefficient / denominator;
        let remainder = quantity.coefficient % denominator;
        self.value.months = self.value.months.checked_add(months)?;
        if remainder != 0 {
            self.add_days(
                Decimal {
                    coefficient: remainder,
                    scale: quantity.scale,
                },
                30,
            )?;
        }
        Some(())
    }

    fn add_days(&mut self, quantity: Decimal, multiplier: i128) -> Option<()> {
        let denominator = pow10(quantity.scale)?;
        let numerator = quantity.coefficient.checked_mul(multiplier)?;
        let days = numerator / denominator;
        let remainder = numerator % denominator;
        self.value.days = self.value.days.checked_add(days)?;
        if remainder != 0 {
            self.value.micros = self.value.micros.checked_add(round_div(
                remainder.checked_mul(MICROS_PER_DAY)?,
                denominator,
            )?)?;
            self.has_time_fields = true;
        }
        normalize_day_carry(&mut self.value)
    }

    fn add_micros(&mut self, quantity: Decimal, multiplier: i128, divisor: i128) -> Option<()> {
        self.value.micros = self
            .value
            .micros
            .checked_add(quantity.scaled_round(multiplier, divisor)?)?;
        Some(())
    }
}

impl Decimal {
    fn parse(value: &str) -> Option<Self> {
        let value = value.trim();
        if value.is_empty() {
            return None;
        }
        let (mantissa, exponent) = match value.find(['e', 'E']) {
            Some(index) => (&value[..index], value[index + 1..].parse::<i32>().ok()?),
            None => (value, 0),
        };
        let (negative, mantissa) = if let Some(rest) = mantissa.strip_prefix('-') {
            (true, rest)
        } else if let Some(rest) = mantissa.strip_prefix('+') {
            (false, rest)
        } else {
            (false, mantissa)
        };
        let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
        if whole.is_empty() && fraction.is_empty()
            || !whole.chars().all(|ch| ch.is_ascii_digit())
            || !fraction.chars().all(|ch| ch.is_ascii_digit())
        {
            return None;
        }
        let digits = format!("{whole}{fraction}");
        let mut coefficient = digits.parse::<i128>().ok()?;
        if negative {
            coefficient = coefficient.checked_neg()?;
        }
        let mut scale = u32::try_from(fraction.len()).ok()?;
        if exponent >= 0 {
            let exponent = exponent as u32;
            if exponent >= scale {
                coefficient = coefficient.checked_mul(pow10(exponent - scale)?)?;
                scale = 0;
            } else {
                scale -= exponent;
            }
        } else {
            scale = scale.checked_add(exponent.unsigned_abs())?;
            pow10(scale)?;
        }
        Some(Self { coefficient, scale })
    }

    fn scaled_round(self, multiplier: i128, divisor: i128) -> Option<i128> {
        let denominator = pow10(self.scale)?.checked_mul(divisor)?;
        round_div(self.coefficient.checked_mul(multiplier)?, denominator)
    }
}

fn round_div(numerator: i128, denominator: i128) -> Option<i128> {
    if denominator <= 0 {
        return None;
    }
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    let doubled = remainder.unsigned_abs().checked_mul(2)?;
    if doubled >= denominator as u128 {
        quotient.checked_add(if numerator < 0 { -1 } else { 1 })
    } else {
        Some(quotient)
    }
}

fn pow10(power: u32) -> Option<i128> {
    10_i128.checked_pow(power)
}

fn normalize_day_carry(value: &mut PostgresInterval) -> Option<()> {
    if value.micros.unsigned_abs() >= MICROS_PER_DAY as u128 {
        let carry = value.micros / MICROS_PER_DAY;
        value.days = value.days.checked_add(carry)?;
        value.micros %= MICROS_PER_DAY;
    }
    Some(())
}

fn apply_qualifier(value: &mut PostgresInterval, qualifier: Option<Qualifier>) -> Option<()> {
    let Some(qualifier) = qualifier else {
        return Some(());
    };
    match qualifier.end {
        Unit::Millennium => {
            value.months = value.months / 12_000 * 12_000;
            value.days = 0;
            value.micros = 0;
        }
        Unit::Century => {
            value.months = value.months / 1_200 * 1_200;
            value.days = 0;
            value.micros = 0;
        }
        Unit::Decade => {
            value.months = value.months / 120 * 120;
            value.days = 0;
            value.micros = 0;
        }
        Unit::Year => {
            value.months = value.months / 12 * 12;
            value.days = 0;
            value.micros = 0;
        }
        Unit::Quarter => {
            value.months = value.months / 3 * 3;
            value.days = 0;
            value.micros = 0;
        }
        Unit::Month => {
            value.days = 0;
            value.micros = 0;
        }
        Unit::Week => {
            value.days = value.days / 7 * 7;
            value.micros = 0;
        }
        Unit::Day => value.micros = 0,
        Unit::Hour => truncate_micros(&mut value.micros, MICROS_PER_HOUR),
        Unit::Minute => truncate_micros(&mut value.micros, MICROS_PER_MINUTE),
        Unit::Second => {
            if let Some(precision) = qualifier.second_precision {
                if precision < 6 {
                    let quantum = pow10(6 - precision)?;
                    value.micros = round_div(value.micros, quantum)?.checked_mul(quantum)?;
                    normalize_day_carry(value)?;
                }
            }
        }
        Unit::Millisecond => truncate_micros(&mut value.micros, 1_000),
        Unit::Microsecond | Unit::Nanosecond => {}
    }
    Some(())
}

fn truncate_micros(value: &mut i128, quantum: i128) {
    *value = *value / quantum * quantum;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parsed(value: &str) -> ParsedInterval {
        let expression = Expression::Interval(Box::new(crate::expressions::Interval {
            this: Some(Expression::string(value)),
            unit: None,
        }));
        let DecomposeOutcome::Parsed(parsed) = decompose(&expression) else {
            panic!("expected parsed interval for {value}");
        };
        parsed
    }

    #[test]
    fn decomposes_postgres_interval_input_families() {
        let cases = [
            ("1 year 2 mons 3 days 04:05:06.7", 14, 3, 14_706_700_000),
            ("36:01", 0, 0, 129_660_000_000),
            ("1-2", 14, 0, 0),
            ("1 02:03:04", 0, 1, 7_384_000_000),
            ("P1Y2M3DT4H5M6.7S", 14, 3, 14_706_700_000),
            ("P0001-02-03T04:05:06.7", 14, 3, 14_706_700_000),
            ("1e-1 day", 0, 0, 8_640_000_000),
            ("1.5 years", 18, 0, 0),
            ("1.5 months", 1, 15, 0),
        ];
        for (input, months, days, micros) in cases {
            assert_eq!(
                parsed(input).value,
                PostgresInterval {
                    months,
                    days,
                    micros,
                },
                "failed for {input}"
            );
        }
    }

    #[test]
    fn applies_postgres_ago_and_independent_field_signs() {
        assert_eq!(
            parsed("1 day -02:00 ago").value,
            PostgresInterval {
                months: 0,
                days: -1,
                micros: 2 * MICROS_PER_HOUR,
            }
        );
        assert_eq!(
            parsed("@ 1 day 2 hours ago").value,
            PostgresInterval {
                months: 0,
                days: -1,
                micros: -2 * MICROS_PER_HOUR,
            }
        );
    }
}
