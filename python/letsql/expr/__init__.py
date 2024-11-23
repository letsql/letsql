from letsql._internal import expr


def __getattr__(name):
    return getattr(expr, name)


def column(name):
    return expr.Expr.column(name)


def col(name):
    return expr.Expr.column(name)
