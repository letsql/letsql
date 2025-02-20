import ibis


def test_explicit_cast(compiler):
    expr = ibis.literal(42).cast("float64")
    yaml_dict = compiler.compile_to_yaml(expr)
    expression = yaml_dict["expression"]

    assert expression["op"] == "Cast"
    assert expression["args"][0]["op"] == "Literal"
    assert expression["args"][0]["value"] == 42
    assert expression["type"]["name"] == "Float64"

    roundtrip_expr = compiler.compile_from_yaml(yaml_dict)
    assert roundtrip_expr.equals(expr)


def test_implicit_cast(compiler):
    i = ibis.literal(1)
    f = ibis.literal(2.5)
    expr = i + f
    yaml_dict = compiler.compile_to_yaml(expr)
    expression = yaml_dict["expression"]

    assert expression["op"] == "Add"
    assert expression["args"][0]["type"]["name"] == "Int8"
    assert expression["args"][1]["type"]["name"] == "Float64"
    assert expression["type"]["name"] == "Float64"

    roundtrip_expr = compiler.compile_from_yaml(yaml_dict)
    assert roundtrip_expr.equals(expr)


def test_string_cast(compiler):
    expr = ibis.literal("42").cast("int64")
    yaml_dict = compiler.compile_to_yaml(expr)
    expression = yaml_dict["expression"]

    assert expression["op"] == "Cast"
    assert expression["args"][0]["value"] == "42"
    assert expression["type"]["name"] == "Int64"

    roundtrip_expr = compiler.compile_from_yaml(yaml_dict)
    assert roundtrip_expr.equals(expr)


def test_timestamp_cast(compiler):
    expr = ibis.literal("2024-01-01").cast("timestamp")
    yaml_dict = compiler.compile_to_yaml(expr)
    expression = yaml_dict["expression"]

    assert expression["op"] == "Cast"
    assert expression["args"][0]["value"] == "2024-01-01"
    assert expression["type"]["name"] == "Timestamp"

    roundtrip_expr = compiler.compile_from_yaml(yaml_dict)
    assert roundtrip_expr.equals(expr)
