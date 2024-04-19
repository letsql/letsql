import inspect


def maybe_unwrap_curry(func, *args, **kwargs):
    # from toolz.curry.__init__
    if (
        hasattr(func, "func")
        and hasattr(func, "args")
        and hasattr(func, "keywords")
        and isinstance(func.args, tuple)
    ):
        _kwargs = {}
        if func.keywords:
            _kwargs.update(func.keywords)
        _kwargs.update(kwargs)
        kwargs = _kwargs
        args = func.args + args
        func = func.func
    elif hasattr(func, "func"):
        func = func.func
    return (func, args, kwargs)


def get_arguments(f, *args, **kwargs):
    (f, args, kwargs) = maybe_unwrap_curry(f, *args, **kwargs)
    signature = inspect.signature(f)
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()
    arguments = bound.arguments
    return arguments


def get_partial_arguments(f, *args, **kwargs):
    (f, args, kwargs) = maybe_unwrap_curry(f, *args, **kwargs)
    signature = inspect.signature(f)
    bound = signature.bind_partial(*args, **kwargs)
    bound.apply_defaults()
    arguments = bound.arguments
    return arguments
