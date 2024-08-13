from __future__ import annotations

import calendar
import math
from functools import partial
from itertools import starmap

import ibis.common.exceptions as com
import ibis.expr.datatypes as dt
import ibis.expr.operations as ops
import sqlglot as sg
import sqlglot.expressions as sge
from ibis.backends.sql.compiler import FALSE, NULL, STAR, SQLGlotCompiler
from ibis.backends.sql.datatypes import DataFusionType
from ibis.backends.sql.dialects import DataFusion
from ibis.backends.sql.rewrites import rewrite_sample_as_filter
from ibis.common.temporal import IntervalUnit, TimestampUnit
from ibis.expr.operations.udf import InputType
from ibis.expr.rewrites import rewrite_stringslice
from ibis.formats.pyarrow import PyArrowType


_UNIX_EPOCH = "1970-01-01T00:00:00Z"


def _replace_offset(offset):
    offset = int(offset)
    return f"{offset + math.copysign(1, offset):.0f}"


class DataFusionCompiler(SQLGlotCompiler):
    __slots__ = ()

    dialect = DataFusion
    type_mapper = DataFusionType
    rewrites = (
        rewrite_sample_as_filter,
        rewrite_stringslice,
        *SQLGlotCompiler.rewrites,
    )

    UNSUPPORTED_OPERATIONS = frozenset(
        (
            ops.ArgMax,
            ops.ArgMin,
            ops.ArrayFilter,
            ops.ArrayMap,
            ops.ArrayZip,
            ops.CountDistinctStar,
            ops.DateDelta,
            ops.GroupConcat,
            ops.MultiQuantile,
            ops.Quantile,
            ops.RowID,
            ops.Strftime,
            ops.TimeDelta,
            ops.TimestampDelta,
        )
    )

    SIMPLE_OPS = {
        ops.ApproxMedian: "approx_median",
        ops.ArrayRemove: "array_remove_all",
        ops.BitAnd: "bit_and",
        ops.BitOr: "bit_or",
        ops.BitXor: "bit_xor",
        ops.Cot: "cot",
        ops.ExtractMicrosecond: "extract_microsecond",
        ops.First: "first_value",
        ops.Last: "last_value",
        ops.Median: "median",
        ops.StringLength: "character_length",
        ops.RandomUUID: "uuid",
        ops.RegexSplit: "regex_split",
        ops.EndsWith: "ends_with",
        ops.ArrayIntersect: "array_intersect",
        ops.ArrayUnion: "array_union",
        ops.ArrayFlatten: "flatten",
        ops.IntegerRange: "range",
        ops.ArrayDistinct: "array_distinct",
        ops.Unnest: "unnest",
        ops.StringToDate: "to_date",
        ops.StringToTimestamp: "to_timestamp",
    }

    def _aggregate(self, funcname: str, *args, where):
        expr = self.f[funcname](*args)
        if where is not None:
            return sg.exp.Filter(this=expr, expression=sg.exp.Where(this=where))
        return expr

    def _to_timestamp(self, value, target_dtype, literal=False):
        tz = (
            f'Some("{timezone}")'
            if (timezone := target_dtype.timezone) is not None
            else "None"
        )
        unit = (
            target_dtype.unit.name.capitalize()
            if target_dtype.scale is not None
            else "Microsecond"
        )
        str_value = str(value) if literal else value
        return self.f.arrow_cast(str_value, f"Timestamp({unit}, {tz})")

    def visit_NonNullLiteral(self, op, *, value, dtype):
        if dtype.is_decimal():
            return self.cast(
                sg.exp.convert(str(value)),
                dt.Decimal(precision=dtype.precision or 38, scale=dtype.scale or 9),
            )
        elif dtype.is_numeric():
            if isinstance(value, float):
                if math.isinf(value):
                    return self.cast("+Inf", dt.float64)
                elif math.isnan(value):
                    return self.cast("NaN", dt.float64)
            return sg.exp.convert(value)
        elif dtype.is_interval():
            if dtype.unit.short in ("ms", "us", "ns"):
                raise com.UnsupportedOperationError(
                    "DataFusion doesn't support subsecond interval resolutions"
                )

            return sg.exp.Interval(
                this=sg.exp.convert(str(value)),
                unit=sg.exp.var(dtype.unit.plural.lower()),
            )
        elif dtype.is_timestamp():
            return self._to_timestamp(value, dtype, literal=True)
        elif dtype.is_date():
            return self.f.date_trunc("day", value.isoformat())
        elif dtype.is_binary():
            return sg.exp.HexString(this=value.hex())
        elif dtype.is_uuid():
            return sge.convert(str(value))
        else:
            return None

    def visit_DefaultLiteral(self, op, *, value, dtype):
        if dtype.is_struct():
            values = [
                self.visit_Literal(
                    ops.Literal(v, field_dtype), value=v, dtype=field_dtype
                )
                for field_dtype, v in zip(dtype.types, value.values())
            ]
            args = (arg for args in zip(value.keys(), values) for arg in args)
            return self.f.named_struct(*args)
        else:
            return super().visit_DefaultLiteral(op, value=value, dtype=dtype)

    def visit_Cast(self, op, *, arg, to):
        if to.is_interval():
            unit = to.unit.name.lower()
            return sg.cast(
                self.f.concat(self.cast(arg, dt.string), f" {unit}"), "interval"
            )
        if to.is_timestamp():
            return self._to_timestamp(arg, to)
        if to.is_decimal():
            return self.f.arrow_cast(arg, f"{PyArrowType.from_ibis(to)}".capitalize())
        return self.cast(arg, to)

    def visit_Arbitrary(self, op, *, arg, where):
        cond = ~arg.is_(None)
        if where is not None:
            cond &= where
        return self.agg.first_value(arg, where=cond)

    def visit_Variance(self, op, *, arg, how, where):
        if how == "sample":
            return self.agg.var_samp(arg, where=where)
        elif how == "pop":
            return self.agg.var_pop(arg, where=where)
        else:
            raise ValueError(f"Unrecognized how value: {how}")

    def visit_StandardDev(self, op, *, arg, how, where):
        if how == "sample":
            return self.agg.stddev_samp(arg, where=where)
        elif how == "pop":
            return self.agg.stddev_pop(arg, where=where)
        else:
            raise ValueError(f"Unrecognized how value: {how}")

    def visit_ScalarUDF(self, op, **kw):
        input_type = op.__input_type__
        if input_type in (InputType.PYARROW, InputType.BUILTIN):
            return self.f[op.__func_name__](*kw.values())
        else:
            raise NotImplementedError(
                f"DataFusion only supports PyArrow UDFs: got a {input_type.name.lower()} UDF"
            )

    def visit_ElementWiseVectorizedUDF(
        self, op, *, func, func_args, input_type, return_type
    ):
        return self.f[func.__name__](*func_args)

    def visit_RegexExtract(self, op, *, arg, pattern, index):
        if not isinstance(op.index, ops.Literal):  # noqa
            raise ValueError(
                "re_extract `index` expressions must be literals. "
                "Arbitrary expressions are not supported in the DataFusion backend"
            )
        return self.f.regexp_match(arg, self.f.concat("(", pattern, ")"))[index]

    def visit_StringFind(self, op, *, arg, substr, start, end):
        if end is not None:
            raise NotImplementedError("`end` not yet implemented")

        if start is not None:
            pos = self.f.strpos(self.f.substr(arg, start + 1), substr)
            return self.f.coalesce(self.f.nullif(pos + start, start), 0)

        return self.f.strpos(arg, substr)

    def visit_RegexSearch(self, op, *, arg, pattern):
        return self.if_(
            sg.or_(arg.is_(NULL), pattern.is_(NULL)),
            NULL,
            self.f.coalesce(
                # null is returned for non-matching patterns, so coalesce to false
                # because that is the desired behavior for ops.RegexSearch
                self.f.array_length(self.f.regexp_match(arg, pattern)) > 0,
                FALSE,
            ),
        )

    def visit_StringContains(self, op, *, haystack, needle):
        return self.f.strpos(haystack, needle) > sg.exp.convert(0)

    def visit_ExtractFragment(self, op, *, arg):
        return self.f.extract_url_field(arg, "fragment")

    def visit_ExtractProtocol(self, op, *, arg):
        return self.f.extract_url_field(arg, "scheme")

    def visit_ExtractAuthority(self, op, *, arg):
        return self.f.extract_url_field(arg, "netloc")

    def visit_ExtractPath(self, op, *, arg):
        return self.f.extract_url_field(arg, "path")

    def visit_ExtractHost(self, op, *, arg):
        return self.f.extract_url_field(arg, "hostname")

    def visit_ExtractQuery(self, op, *, arg, key):
        if key is not None:
            return self.f.extract_query_param(arg, key)
        return self.f.extract_query(arg)

    def visit_ExtractUserInfo(self, op, *, arg):
        return self.f.extract_user_info(arg)

    def visit_ExtractYearMonthQuarterDay(self, op, *, arg):
        skip = len("Extract")
        part = type(op).__name__[skip:].lower()
        return self.f.date_part(part, arg)

    visitExtractYear = visit_ExtractYearMonthQuarterDay
    visit_ExtractMonth = visit_ExtractYearMonthQuarterDay
    visit_ExtractQuarter = visit_ExtractYearMonthQuarterDay
    visit_ExtractDay = visit_ExtractYearMonthQuarterDay

    def visit_ExtractDayOfYear(self, op, *, arg):
        return self.f.date_part("doy", arg)

    def visit_DayOfWeekIndex(self, op, *, arg):
        return (self.f.date_part("dow", arg) + 6) % 7

    def visit_DayOfWeekName(self, op, *, arg):
        return sg.exp.Case(
            this=sge.paren(self.f.date_part("dow", arg) + 6, copy=False) % 7,
            ifs=list(starmap(self.if_, enumerate(calendar.day_name))),
        )

    def visit_Date(self, op, *, arg):
        return self.f.date_trunc("day", arg)

    def visit_ExtractWeekOfYear(self, op, *, arg):
        return self.f.date_part("week", arg)

    def visit_TimestampTruncate(self, op, *, arg, unit):
        if unit in (
            IntervalUnit.MILLISECOND,
            IntervalUnit.MICROSECOND,
            IntervalUnit.NANOSECOND,
        ):
            raise com.UnsupportedOperationError(
                f"The function is not defined for time unit {unit}"
            )

        return self.f.date_trunc(unit.name.lower(), arg)

    def visit_ExtractEpochSeconds(self, op, *, arg):
        if op.arg.dtype.is_date():
            return self.f.extract_epoch_seconds_date(arg)
        elif op.arg.dtype.is_timestamp():
            return self.f.extract_epoch_seconds_timestamp(arg)
        else:
            raise com.OperationNotDefinedError(
                f"The function is not defined for {op.arg.dtype}"
            )

    def visit_ExtractMinute(self, op, *, arg):
        if op.arg.dtype.is_date():
            return self.f.date_part("minute", arg)
        elif op.arg.dtype.is_time():
            return self.f.extract_minute_time(arg)
        elif op.arg.dtype.is_timestamp():
            return self.f.extract_minute_timestamp(arg)
        else:
            raise com.OperationNotDefinedError(
                f"The function is not defined for {op.arg.dtype}"
            )

    def visit_ExtractMillisecond(self, op, *, arg):
        if op.arg.dtype.is_time():
            return self.f.extract_millisecond_time(arg)
        elif op.arg.dtype.is_timestamp():
            return self.f.extract_millisecond_timestamp(arg)
        else:
            raise com.OperationNotDefinedError(
                f"The function is not defined for {op.arg.dtype}"
            )

    def visit_ExtractHour(self, op, *, arg):
        if op.arg.dtype.is_date() or op.arg.dtype.is_timestamp():
            return self.f.date_part("hour", arg)
        elif op.arg.dtype.is_time():
            return self.f.extract_hour_time(arg)
        else:
            raise com.OperationNotDefinedError(
                f"The function is not defined for {op.arg.dtype}"
            )

    def visit_ExtractSecond(self, op, *, arg):
        if op.arg.dtype.is_date() or op.arg.dtype.is_timestamp():
            return self.f.extract_second_timestamp(arg)
        elif op.arg.dtype.is_time():
            return self.f.extract_second_time(arg)
        else:
            raise com.OperationNotDefinedError(
                f"The function is not defined for {op.arg.dtype}"
            )

    def visit_ArrayRepeat(self, op, *, arg, times):
        return self.f.flatten(self.f.array_repeat(arg, times))

    def visit_ArrayPosition(self, op, *, arg, other):
        return self.f.coalesce(self.f.array_position(arg, other), 0)

    def visit_Covariance(self, op, *, left, right, how, where):
        x = op.left
        if x.dtype.is_boolean():
            left = self.cast(left, dt.float64)

        y = op.right
        if y.dtype.is_boolean():
            right = self.cast(right, dt.float64)

        if how == "sample":
            return self.agg.covar_samp(left, right, where=where)
        elif how == "pop":
            return self.agg.covar_pop(left, right, where=where)
        else:
            raise ValueError(f"Unrecognized how = `{how}` value")

    def visit_Correlation(self, op, *, left, right, where, how):
        x = op.left
        if x.dtype.is_boolean():
            left = self.cast(left, dt.float64)

        y = op.right
        if y.dtype.is_boolean():
            right = self.cast(right, dt.float64)

        return self.agg.corr(left, right, where=where)

    def visit_IsNan(self, op, *, arg):
        return sg.and_(arg.is_(sg.not_(NULL)), self.f.isnan(arg))

    def visit_ArrayStringJoin(self, op, *, sep, arg):
        return self.f.array_join(arg, sep)

    def visit_FindInSet(self, op, *, needle, values):
        return self.f.coalesce(
            self.f.array_position(self.f.make_array(*values), needle), 0
        )

    def visit_TimestampFromUNIX(self, op, *, arg, unit):
        if unit == TimestampUnit.SECOND:
            return self.f.from_unixtime(arg)
        elif unit in (
            TimestampUnit.MILLISECOND,
            TimestampUnit.MICROSECOND,
            TimestampUnit.NANOSECOND,
        ):
            return self.f.arrow_cast(arg, f"Timestamp({unit.name.capitalize()}, None)")
        else:
            raise com.UnsupportedOperationError(f"Unsupported unit {unit}")

    def visit_DateFromYMD(self, op, *, year, month, day):
        return self.cast(
            self.f.concat(
                self.f.lpad(self.cast(self.cast(year, dt.int64), dt.string), 4, "0"),
                "-",
                self.f.lpad(self.cast(self.cast(month, dt.int64), dt.string), 2, "0"),
                "-",
                self.f.lpad(self.cast(self.cast(day, dt.int64), dt.string), 2, "0"),
            ),
            dt.date,
        )

    def visit_TimestampFromYMDHMS(
        self, op, *, year, month, day, hours, minutes, seconds, **_
    ):
        return self.f.to_timestamp_micros(
            self.f.concat(
                self.f.lpad(self.cast(self.cast(year, dt.int64), dt.string), 4, "0"),
                "-",
                self.f.lpad(self.cast(self.cast(month, dt.int64), dt.string), 2, "0"),
                "-",
                self.f.lpad(self.cast(self.cast(day, dt.int64), dt.string), 2, "0"),
                "T",
                self.f.lpad(self.cast(self.cast(hours, dt.int64), dt.string), 2, "0"),
                ":",
                self.f.lpad(self.cast(self.cast(minutes, dt.int64), dt.string), 2, "0"),
                ":",
                self.f.lpad(self.cast(self.cast(seconds, dt.int64), dt.string), 2, "0"),
                ".000000Z",
            )
        )

    def visit_IsInf(self, op, *, arg):
        return sg.and_(sg.not_(self.f.isnan(arg)), self.f.abs(arg).eq(self.POS_INF))

    def visit_ArrayIndex(self, op, *, arg, index):
        return self.f.array_element(arg, index + self.cast(index >= 0, op.index.dtype))

    def visit_StringConcat(self, op, *, arg):
        any_args_null = (a.is_(NULL) for a in arg)
        return self.if_(
            sg.or_(*any_args_null), self.cast(NULL, dt.string), self.f.concat(*arg)
        )

    def visit_Aggregate(self, op, *, parent, groups, metrics):
        """Support `GROUP BY` expressions in `SELECT` since DataFusion does not."""
        quoted = self.quoted
        metrics = tuple(self._cleanup_names(metrics))

        if groups:
            # datafusion doesn't support count distinct aggregations alongside
            # computed grouping keys so create a projection of the key and all
            # existing columns first, followed by the usual group by
            #
            # analogous to a user calling mutate -> group_by
            cols = list(
                map(
                    partial(
                        sg.column,
                        table=sg.to_identifier(parent.alias, quoted=quoted),
                        quoted=quoted,
                    ),
                    # can't use set subtraction here since the schema keys'
                    # order matters and set subtraction doesn't preserve order
                    (k for k in op.parent.schema.keys() if k not in groups),
                )
            )
            table = (
                sg.select(*cols, *self._cleanup_names(groups))
                .from_(parent)
                .subquery(parent.alias)
            )

            # datafusion lower cases all column names internally unless quoted so
            # quoted=True is required here for correctness
            by_names_quoted = tuple(
                sg.column(key, table=getattr(value, "table", None), quoted=quoted)
                for key, value in groups.items()
            )
            selections = by_names_quoted + metrics
        else:
            selections = metrics or (STAR,)
            table = parent

        sel = sg.select(*selections).from_(table)

        if groups:
            sel = sel.group_by(*by_names_quoted)

        return sel

    def visit_ArraySlice(self, op, *, arg, start, stop):
        array_length = self.f.array_length(arg)
        start = self.f.coalesce(start, 0)
        stop = self.f.coalesce(stop, array_length + 1)
        return self.f.array_slice(
            arg,
            self.if_(
                start < 0,
                self.if_(self.f.abs(start) >= array_length, 0, start),
                start + 1,
            ),
            self.if_(stop < 0, stop - 1, stop),
        )

    def visit_TimestampNow(self, op):
        return self.f.to_timestamp_micros(self.cast(self.f.now(), dt.string))

    def visit_HexDigest(self, op, *, arg, how):
        return self.f.encode(self.f.digest(arg, how), "hex")

    def visit_TypeOf(self, op, *, arg):
        return self.f.arrow_typeof(arg)

    def visit_BitwiseNot(self, op, *, arg):
        # https://stackoverflow.com/q/69648488/4001592
        return sge.BitwiseXor(this=arg, expression=sg.exp.convert(-1))

    def visit_Clip(self, op, *, arg, lower, upper):
        ifs = []
        if lower is not None:
            lower_case = self.if_(arg < lower, lower)
            ifs.append(lower_case)
        if upper is not None:
            upper_case = self.if_(arg > upper, upper)
            ifs.append(upper_case)

        return sg.exp.Case(ifs=ifs, default=arg)

    def visit_IntervalFromInteger(self, op, *, arg, unit):
        unit = unit.name.lower()
        return sg.cast(self.f.concat(self.cast(arg, dt.string), f" {unit}"), "interval")

    def visit_Greatest(self, op, *, arg):
        return self.f.greatest(*arg)

    def visit_Least(self, op, *, arg):
        return self.f.least(*arg)

    def visit_TimestampBucket(self, op, *, arg, interval, offset):
        if offset is None:
            return self.f.date_bin(interval, arg)
        else:
            this = offset.this
            this.set("this", _replace_offset(this.this))
            offset = (
                self.f.arrow_cast(_UNIX_EPOCH, "Timestamp(Nanosecond, None)") - offset
            )
        return self.f.date_bin(interval, arg, offset)

    def visit_StructField(self, op, *, arg, field):
        return sge.Bracket(this=arg, expressions=[sg.exp.convert(field)])

    def visit_StructColumn(self, op, *, names, values):
        args = (arg for args in zip(map(sg.exp.convert, names), values) for arg in args)
        return self.f.named_struct(*args)

    def visit_SegmentAnything(self, op, *, arg, model_name, seed):
        return self.f.segment_anything(sg.exp.convert(model_name), arg, seed)

    def visit_Rotate90(self, op, *, arg):
        return self.f.rotate90(arg)
