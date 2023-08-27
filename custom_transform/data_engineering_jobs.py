#dataframe variable is initialized in the transform_data function
dataframe = dataframe[['PositionID',
                       'PositionTitle',
                       'PositionURI',
                       'PositionLocation',
                       'PositionRemuneration']]  # get columns that we need
#rename column to snake_case format
dataframe.rename(columns={'PositionID': 'position_id',
                          'PositionTitle': 'position_title',
                          'PositionURI': 'position_uri',
                          'PositionLocation': 'position_location',
                          'PositionRemuneration': 'position_remuneration'}, inplace=True)
# capitalized on each word, e.g. program manager -> Program Manager
dataframe['position_title'] = dataframe['position_title'].apply(
    lambda x: x.title())

#processing position_location: change its keys from camelCase format to snake_case
arr_position_location = list(dataframe['position_location'])
new_position_location_arr = []
position_location_key = {
        "LocationName" : "location_name",
        "CountryCode" : "country_code",
        "CountrySubDivisionCode" : "country_sub_division_code",
        "CityName" : "city_name",
        "Longitude" : "longitude",
        "Latitude" : "latitude"
}
for row in arr_position_location:
    new_row = []
    for old_location in row:
        new_location = {}
        for old_key in position_location_key.keys():
            if old_key in old_location: #only assign value if key is available
                new_location[position_location_key[old_key]] = old_location[old_key]
        new_row.append(new_location)
    new_position_location_arr.append(new_row)
dataframe['position_location'] = new_position_location_arr

#do the same thing for position_remuneration + change salary range data type to float instead of string
arr_position_remuneration = list(dataframe['position_remuneration'])
new_position_remuneration_arr = []
position_remuneration_key = {
        "MinimumRange" : "minimum_range",
        "MaximumRange" : "maximum_range",
        "RateIntervalCode" : "rate_interval_code",
        "Description":"description"
}
for row in arr_position_remuneration:
    new_row = []
    for old_remuneration in row:
        new_remuneration = {}
        for old_key in position_remuneration_key.keys():
            if old_key in old_remuneration: #only assign value if key is available
                if old_key in ['MinimumRange','MaximumRange']:
                    new_remuneration[position_remuneration_key[old_key]] = float(old_remuneration[old_key])
                else:
                    new_remuneration[position_remuneration_key[old_key]] = old_remuneration[old_key]
        new_row.append(new_remuneration)
    new_position_remuneration_arr.append(new_row)
dataframe['position_remuneration'] = new_position_remuneration_arr
#make sure that we save our final transformation result in 'dataframe' variable again