import pytest
import sys

sys.path.append("./app")
from app.file_parser import map_url_params_to_columns


def test_map_url_params_to_columns():
    url = "http://example.com?a_bucket=1&a_type=2&a_source=3&a_v=4&a_g_campaignid=5&a_g_keyword=6&a_g_adgroupid=7&a_g_creative=8"
    expected_columns = {
        "url": url,
        "ad_bucket": "1",
        "ad_type": "2",
        "ad_source": "3",
        "schema_version": "4",
        "ad_campaign_id": "5",
        "ad_keyword": "6",
        "ad_adgroup_id": "7",
        "ad_creative": "8",
    }
    assert map_url_params_to_columns(url) == expected_columns
