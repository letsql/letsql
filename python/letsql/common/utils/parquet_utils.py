import pyarrow.parquet as pq

from letsql.expr import col


def get_pq_sort_order(path):
    sorting_columns = pq.read_metadata(path).row_group(0).sorting_columns
    return sorting_columns


def ordering_to_file_sort_order(ordering, null_placement="at_start"):
    file_sort_order = [
        [
            col(field).sort(
                ascending=order == "ascending",
                nulls_first=null_placement == "at_start",
            )
            for (field, order) in ordering
        ]
    ]
    return file_sort_order


def pq_order_to_df_order(schema, sorting_columns):
    (ordering, null_placement) = pq.SortingColumn.to_ordering(schema, sorting_columns)
    return ordering_to_file_sort_order(ordering)


def get_df_sort_order(path):
    schema = pq.read_metadata(path).schema.to_arrow_schema()
    sorting_columns = get_pq_sort_order(path)
    return pq_order_to_df_order(schema, sorting_columns)
