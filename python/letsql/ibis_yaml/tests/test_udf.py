import pytest

import letsql.ibis_yaml
import letsql.ibis_yaml.utils
import letsql.vendor.ibis as ibis


def test_built_in_udf_properties(compiler):
    t = ibis.table({"a": "int64"}, name="t")

    @ibis.udf.scalar.builtin
    def add_one(x: int) -> int:
        return x + 1

    expr = t.mutate(new=add_one(t.a))
    yaml_dict = compiler.to_yaml(expr)
    roundtrip_expr = compiler.from_yaml(yaml_dict)

    original_mutation = expr.op()
    roundtrip_mutation = roundtrip_expr.op()

    original_udf = original_mutation.values["new"]
    roundtrip_udf = roundtrip_mutation.values["new"]

    assert original_udf.__func_name__ == roundtrip_udf.__func_name__
    assert original_udf.__input_type__ == roundtrip_udf.__input_type__
    assert original_udf.dtype == roundtrip_udf.dtype
    assert len(original_udf.args) == len(roundtrip_udf.args)

    for orig_arg, rt_arg in zip(original_udf.args, roundtrip_udf.args):
        assert orig_arg.dtype == rt_arg.dtype


def test_compiler_raises(compiler):
    t = ibis.table({"a": "int64"}, name="t")

    @ibis.udf.scalar.python
    def add_one(x: int) -> int:
        pass

    expr = t.mutate(new=add_one(t.a))
    with pytest.raises(NotImplementedError):
        compiler.to_yaml(expr)


@pytest.mark.xfail(
    reason="UDFs do not have the same memory address when pickled/unpickled"
)
def test_built_in_udf(compiler):
    # (Pdb) diffs[3][2].args[0] == diffs[3][1].args[0]
    # False
    # (Pdb) diffs[3][2].args[0]
    # <ibis.expr.operations.relations.Project object at 0x7ffff48f53d0>
    # (Pdb) diffs[3][2].args[0].args
    # (<ibis.expr.operations.relations.UnboundTable object at 0x7ffff48f5310>, {'a': <ibis.expr.operations.relations.Field object at 0x7ffff490cbb0>, 'new': <tests.test_udf.add_one_1 object at 0x7ffff48f5550>})
    # (Pdb) diffs[3][1].args[0].args
    # (<ibis.expr.operations.relations.UnboundTable object at 0x7ffff48f45f0>, {'a': <ibis.expr.operations.relations.Field object at 0x7ffff490cb40>, 'new': <tests.test_udf.add_one_1 object at 0x7ffff48f49b0>})
    t = ibis.table({"a": "int64"}, name="t")

    @ibis.udf.scalar.builtin
    def add_one(x: int) -> int:
        pass

    expr = t.mutate(new=add_one(t.a))
    yaml_dict = compiler.to_yaml(expr)
    roundtrip_expr = compiler.from_yaml(yaml_dict)
    print(f"Original {expr}")
    print(f"Roundtrip {roundtrip_expr}")
    letsql.ibis_yaml.utils.diff_ibis_exprs(expr, roundtrip_expr)

    assert roundtrip_expr.equals(expr)
