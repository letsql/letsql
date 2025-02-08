import ibis
import ibis.expr.operations as ops


def test_scalar_subquery(compiler, t):
    expr = ops.ScalarSubquery(t.c.mean().as_table()).to_expr()
    yaml_dict = compiler.compile_to_yaml(expr)

    assert yaml_dict["op"] == "ScalarSubquery"
    assert yaml_dict["args"][0]["op"] == "Aggregate"

    roundtrip_expr = compiler.compile_from_yaml(yaml_dict)
    assert roundtrip_expr.equals(expr)


def test_exists_subquery(compiler):
    t1 = ibis.table(dict(a="int", b="string"), name="t1")
    t2 = ibis.table(dict(a="int", c="float"), name="t2")

    filtered = t2.filter(t2.a == t1.a)
    expr = ops.ExistsSubquery(filtered).to_expr()
    yaml_dict = compiler.compile_to_yaml(expr)

    assert yaml_dict["op"] == "ExistsSubquery"
    assert yaml_dict["rel"]["op"] == "Filter"

    roundtrip_expr = compiler.compile_from_yaml(yaml_dict)
    assert roundtrip_expr.equals(expr)


def test_in_subquery(compiler):
    t1 = ibis.table(dict(a="int", b="string"), name="t1")
    t2 = ibis.table(dict(a="int", c="float"), name="t2")

    expr = ops.InSubquery(t1.select("a"), t2.a).to_expr()
    yaml_dict = compiler.compile_to_yaml(expr)

    assert yaml_dict["op"] == "InSubquery"
    assert yaml_dict["type"]["name"] == "Boolean"

    roundtrip_expr = compiler.compile_from_yaml(yaml_dict)
    assert roundtrip_expr.equals(expr)
