## Pipe Module

The pipe module introduces 3 changes to LETSQL, with the objective of letting users of the library
to quickly build data pipelines. These changes are:

- Partial Ibis Expressions
- Pipe Operator
- Workflow Tools Module

### Partial Ibis Expressions

The Partial Ibis Expressions allows the user to create functions that receive a table to apply transformations 
on that table, the transformations can be:

1. join
2. mutate
3. select
4. filter
5. head

This simplifies the creation of the steps or tasks of a pipeline, for example something like:

```python
@task
def load(table):
    pipeline = (
        table.join(right, "a")
        .select("a", bx="b", cx="c")
        .mutate(a2=_.a * 2, bcx=_.bx + _.cx)
        .limit(5)
    )
    return pipeline
```

becomes:

```python
load = (
    join(right, "a")
    .select("a", bx="b", cx="c")
    .mutate(a2=_.a * 2, bcx=_.bx + _.cx)
    .limit(5)
)
```

#### Proposal: Pipe Method for Partial Ibis Expressions

Perhaps is worthy to add a pipe method to `PartialExpr` class, similar to the one in `ibis` but with the 
added benefit of receiving multiple functions instead of single one.

### Pipe Operator 

The objective of the pipe `|` operator is to be able to "glue" together multiple functions, including ibis partial 
expressions. The semantics is similar to the `pipe` function from `ibis`, it will compose two functions together over
an `ibis` table. 

By using the pipe operator we simplify the construction of data pipelines, because it enables the user to pipe together
multiple partial expressions in one large pipeline, for example:

```python
    pipeline = (
        left
        | join(right, "a")
        | select("a", bx="b", cx="c")
        | mutate(a2=_.a * 2, bcx=_.bx + _.cx)
        | limit(5)
    )
```

Notice the use of each `|` pipe to "glue" together each partial expression, it should be used for that. 

### Proposal: Pipe Operator should receive multiple arguments

A nice syntactic sugar would be to let the `|` receive multiple arguments, for example:

```python
    pipeline = (
        [left, right]
        | join("a")
        | select("a", bx="b", cx="c")
        | mutate(a2=_.a * 2, bcx=_.bx + _.cx)
        | limit(5)
    )
```

### Workflow Tools Module

The workflow tools module should include functions to manipulate and execute a data pipeline:

- execute: executes the workflow using a local engine (such as DuckDB, or DataFusion)
- [proposal] tee: writes the pyarrow_batches of an expr to multiple outputs returns the expr bound to the last connection
- [proposal] read: read an `ibis.Table` from a file or a connection
- [proposal] write: write to a file or to a connection
- [proposal] cache: caches an expr to a particular connection or file, can be thought of as write followed by a read.
- [proposal] redirect: it will be like the ">" operator

