import unittest

import pandas as pd
from nose.tools import raises

from src.features.transformations import driver_distance_to_pickup, hour_of_day


class TestTransformationFeatures(unittest.TestCase):
    @raises(TypeError)
    def test_hour_of_day(self):
        df = pd.DataFrame({"event_timestamp": ["2015-05-12 05:25:23.904 UTC"]})
        got = hour_of_day(df)
        self.assertIn("event_hour", got.columns)
        self.assertEqual(len(got.columns), 2)
        self.assertEqual(got["event_hour"].tolist(), [5])

    @raises(KeyError)
    def test_transform_with_invalid_key(self):
        df = pd.DataFrame({"invalid_key": [1, 2, 3]})
        self.assertRaises(KeyError, driver_distance_to_pickup(df))
        self.assertRaises(KeyError, hour_of_day(df))
