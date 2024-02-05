if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    
    columns = {'VendorID':'vendor_id',
           'PULocationID':'pickup_location_id',
           'DOLocationID':'dropoff_location_id',
           'RatecodeID':'ratecode_id',
           }

    data = (data
            .query("passenger_count!=0")
            .query("trip_distance!=0")
            .assign(lpep_pickup_date = lambda x: x['lpep_pickup_datetime'].dt.date)
            .rename(columns=columns)
            )
    return data


@test
def test_output(output, *args) -> None:
    """
    Test that transformer has an output
    """
    assert output is not None, 'The output is undefined'

@test
def test_output(output, *args) -> None:
    """
    Test if vendor_id is onthe of the existing values
    """
    assert output['vendor_id'].isin([1, 2]).all(), "vendor_id must be 1 or 2"

@test
def test_output(output, *args) -> None:
    """
    Test if passenger_count is greater than 0
    """
    assert (output['passenger_count'] > 0).all(), "passenger_count must be greater than 0"


@test
def test_output(output, *args) -> None:
    """
    Test if trip_distance is greater than 0
    """
    assert (output['trip_distance'] > 0).all(), "trip_distance must be greater than 0"

