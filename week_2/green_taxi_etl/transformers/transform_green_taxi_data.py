import pandas as pd
from stringcase import pascalcase, snakecase

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):

    ##add date column
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    ##rename column
    df = pd.DataFrame(data)

    ## variant 1
    ## not fully matched because VendorID became vendor_i_d
    ##df.columns = [snakecase(col) for col in df.columns]

    ## variant 2
    ## not fully matched because VendorID became vendorid   
    ##df.columns = [col.lower() for col in df.columns]

    ## variant 3
    ## not elegant but it works
    df.rename(columns={'VendorID': 'vendor_id'}, inplace=True)
    df.rename(columns={'RatecodeID': 'ratecode_id'}, inplace=True)
    df.rename(columns={'PULocationID': 'pu_location_id'}, inplace=True)
    df.rename(columns={'DOLocationID': 'do_location_id'}, inplace=True)

    print(f"Preprocessing rows with zero passengers: { data[['passenger_count']].isin([0]).sum() }")
    print(f"Preprocessing rows with zero trip distance: { data[['trip_distance']].isin([0]).sum() }")

    # check vendor_id values
    vendor_id_values = df['vendor_id'].unique().tolist()
    print(vendor_id_values)

    # null to 0
    data['vendor_id'].fillna(0, inplace=True)

    # check vendor_id values
    vendor_id_values1 = df['vendor_id'].unique().tolist()
    print(vendor_id_values1)

    return data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)] #& (data['vendor_id'] > 0)]


@test
def test_output(output, *args) -> None:

    assert output['passenger_count'].isin([0]).sum() == 0, 'There are rides with 0 passengers'
    assert output['trip_distance'].isin([0]).sum() == 0, 'There are rides with 0 trip distance'
    assert output['vendor_id'].isin([0]).sum() == 0, 'There are rides with empty vendor_id. Please check the data'
    



