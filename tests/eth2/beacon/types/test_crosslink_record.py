from eth2.beacon.types.crosslink_records import (
    CrosslinkRecord,
)


def test_defaults(sample_crosslink_record_params):
    crosslink = CrosslinkRecord(**sample_crosslink_record_params)
    assert crosslink.epoch == sample_crosslink_record_params['epoch']
    assert crosslink.crosslink_data_root == sample_crosslink_record_params['crosslink_data_root']
    print(CrosslinkRecord.get_static_size())
    print('3 * CrosslinkRecord.get_static_size()', 3 * CrosslinkRecord.get_static_size())
