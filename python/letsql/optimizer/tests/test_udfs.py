import io

import pandas as pd
import pyarrow as pa
import pytest
import requests
from PIL import Image


from letsql.internal import SessionContext
from letsql.tests.util import assert_frame_equal

import letsql as ls

IMAGE_FORMAT = "JPEG"
TULIPS_URL = "https://github.com/mohammadimtiazz/standard-test-images-for-Image-Processing/blob/master/standard_test_images/tulips.png?raw=true"


@pytest.fixture
def images_table():
    image = Image.open(requests.get(TULIPS_URL, stream=True).raw)
    output = io.BytesIO()
    image.save(output, format=IMAGE_FORMAT)

    table = pa.Table.from_arrays(
        [
            pa.array(["tulips.png", "tulips2.png"]),
            pa.array([output.getvalue(), output.getvalue()], type=pa.binary()),
        ],
        names=["name", "data"],
    )

    return table


def test_tensor_mean_all():
    context = SessionContext()
    query = "select tensor_mean_all(make_array(1.0, 2.0, 3.0));"
    actual = context.sql(query).to_pandas().rename(lambda x: "tmp", axis="columns")
    expected = pd.DataFrame({"tmp": [2.0]})

    assert_frame_equal(actual, expected)


def test_tensor_mean_all_over_matrix():
    context = SessionContext()
    query = "select tensor_mean_all(make_array(make_array(1.0,2.0,3.0), make_array(1.0,2.0,3.0)));"
    actual = context.sql(query).to_pandas().rename(lambda x: "tmp", axis="columns")
    expected = pd.DataFrame({"tmp": [2.0]})

    assert_frame_equal(actual, expected)


def test_segment_anything(images_table):
    context = SessionContext()
    context.register_record_batches("images", [images_table.to_batches()])

    rows = context.sql(
        """
        SELECT segment_anything('mobile_sam-tiny-vitt.safetensors', data, make_array(0.5, 0.6)) as segmented
        FROM images
        LIMIT 3
        """
    ).to_pylist()

    output = [Image.open(io.BytesIO(row["segmented"])) for row in rows]
    assert output is not None
    assert all(image.format == IMAGE_FORMAT for image in output)


def test_segment_anything_op(images_table):
    con = ls.connect()
    images = con.register(images_table, table_name="images")

    expr = images.data.segment_anything("mobile_sam-tiny-vitt.safetensors", [0.5, 0.6])

    segmented = expr.execute()
    assert segmented is not None

    output = [Image.open(io.BytesIO(data)) for data in segmented.to_list()]
    assert output is not None
    assert all(image.format == IMAGE_FORMAT for image in output)


def test_rotate(images_table):
    context = SessionContext()
    context.register_record_batches("images", [images_table.to_batches()])

    rows = context.sql(
        """
        SELECT image_rotate(data) as rotated
        FROM images
        LIMIT 3
        """
    ).to_pylist()

    output = [Image.open(io.BytesIO(row["rotated"])) for row in rows]
    assert output is not None
    assert all(image.format == IMAGE_FORMAT for image in output)


def test_rotate_op(images_table):
    con = ls.connect()
    images = con.register(images_table, table_name="images")

    expr = images.data.rotate()

    rotated = expr.execute()
    assert rotated is not None

    output = [Image.open(io.BytesIO(data)) for data in rotated.to_list()]
    assert output is not None
    assert all(image.format == IMAGE_FORMAT for image in output)
