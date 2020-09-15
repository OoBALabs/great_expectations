import logging
import pytest


# TODO: <Alex>Will, do we need these?</Alex>
# from great_expectations.execution_environment.execution_environment import (
#     ExecutionEnvironment as exec,
# )
#
# from great_expectations.execution_environment.data_connector import (
#     DataConnector,
#     FilesDataConnector,
# )

from great_expectations.execution_environment.data_connector.partitioner import RegexPartitioner

try:
    from unittest import mock
except ImportError:
    import mock

logger = logging.getLogger(__name__)

"""
asset_param = {
    "test_asset": {
        "partition_regex": r"file_(.*)_(.*).csv",
        "partition_param": ["year", "file_num"],
        "partition_delimiter": "-",
        "reader_method": "read_csv",
    }
}

"""
batch_paths = [
  "my_dir/alex_20200809_1000.csv",
  "my_dir/eugene_20200809_1500.csv",
  "my_dir/james_20200811_1009.csv",
  "my_dir/abe_20200809_1040.csv",
  "my_dir/will_20200809_1002.csv",
  "my_dir/james_20200713_1567.csv",
  "my_dir/eugene_20201129_1900.csv",
  "my_dir/will_20200810_1001.csv",
  "my_dir/james_20200810_1003.csv",
  "my_dir/alex_20200819_1300.csv",
]


def test_regex_partitioner():
    my_partitioner = RegexPartitioner(name="mine_all_mine")
    regex = r".*/(.*)_(.*)_(.*).csv"

    # test 1: no regex configured. we  raise error
    with pytest.raises(ValueError) as exc:
        partitions = my_partitioner.get_available_partitions(batch_paths)

    # set the regex
    my_partitioner.regex = regex
    returned_partitions = my_partitioner.get_available_partitions(batch_paths)
    assert returned_partitions == [
        {'partition_definition': {'group_0': 'alex', 'group_1': '20200809', 'group_2': '1000'}, 'partition_key': 'alex-20200809-1000'},
        {'partition_definition': {'group_0': 'eugene', 'group_1': '20200809', 'group_2': '1500'}, 'partition_key': 'eugene-20200809-1500'},
        {'partition_definition': {'group_0': 'james', 'group_1': '20200811', 'group_2': '1009'}, 'partition_key': 'james-20200811-1009'},
        {'partition_definition': {'group_0': 'abe', 'group_1': '20200809', 'group_2': '1040'}, 'partition_key': 'abe-20200809-1040'},
        {'partition_definition': {'group_0': 'will', 'group_1': '20200809', 'group_2': '1002'}, 'partition_key': 'will-20200809-1002'},
        {'partition_definition': {'group_0': 'james', 'group_1': '20200713', 'group_2': '1567'}, 'partition_key': 'james-20200713-1567'},
        {'partition_definition': {'group_0': 'eugene', 'group_1': '20201129', 'group_2': '1900'}, 'partition_key': 'eugene-20201129-1900'},
        {'partition_definition': {'group_0': 'will', 'group_1': '20200810', 'group_2': '1001'}, 'partition_key': 'will-20200810-1001'},
        {'partition_definition': {'group_0': 'james', 'group_1': '20200810', 'group_2': '1003'}, 'partition_key': 'james-20200810-1003'},
        {'partition_definition': {'group_0': 'alex', 'group_1': '20200819', 'group_2': '1300'}, 'partition_key': 'alex-20200819-1300'}
    ]

    # partition keys
    returned_partition_keys = my_partitioner.get_available_partition_keys(batch_paths)
    assert returned_partition_keys == [
        'alex-20200809-1000',
        'eugene-20200809-1500',
        'james-20200811-1009',
        'abe-20200809-1040',
        'will-20200809-1002',
        'james-20200713-1567',
        'eugene-20201129-1900',
        'will-20200810-1001',
        'james-20200810-1003',
        'alex-20200819-1300'
    ]
