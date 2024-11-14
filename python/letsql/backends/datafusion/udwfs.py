from letsql.internal import WindowEvaluator
import letsql.internal as df
import pyarrow as pa


def default_window_evaluator(func):
    class MyWindowEvaluator(df.WindowEvaluator):
        def evaluate_all(self, values: list[pa.Array], num_rows: int) -> pa.Array:
            values = values[0].slice(0, num_rows)
            return func(values)

    return MyWindowEvaluator()


def get_range_window_evaluator(config, func):
    class RangedWindowEvaluator(df.WindowEvaluator):
        def supports_bounded_execution(self) -> bool:
            return True

        def get_range(self, idx: int, num_rows: int) -> tuple[int, int]:
            return config["range"](idx)

        def evaluate(
            self, values: list[pa.Array], eval_range: tuple[int, int]
        ) -> pa.Scalar:
            (start, stop) = eval_range
            sliced = values[0].slice(start, (stop - start) + 1)
            return func(sliced)

    return RangedWindowEvaluator()


def get_window_evaluator(config, func) -> WindowEvaluator:
    if "range" in config and config["evaluation"] == "one":
        return get_range_window_evaluator(config, func)

    return default_window_evaluator(func)
