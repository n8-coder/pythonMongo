from datetime import datetime  # datetime library to convert to date objects
from pprint import pprint  # pprint library is used to make the output look more pretty

import pandas as pd  # pandas library to read and manipulate data from spreadsheets
from pymongo import MongoClient  # pymongo library to talk to MongoDB

# Connect to MongoDB - Note: Change connection string as needed
client = MongoClient(port=27017)
db = client.biodiversity

# Read the DigiVol data into a DataFrame
data = pd.read_csv("file:///C:\\Users\\noliyath\\workspace\\KI_CameraTraps\\Project-172211812-DwC.csv")
print(data.columns)


# Private function to get datetime object
# Default format yyyymmddHHMMSS
def _date_time(file_name):
    date_string = file_name.split("_")[5]
    return _to_dateobject(date_string, '%Y%m%d%H%M%S')


def _to_dateobject(value, fmt):
    return datetime.strptime(value, fmt)


# Private function to get camera name from filename
def _camera_name(file_name):
    return file_name.split("_")[4]


def _find_record(row):
    db.imagevalidations.find_one({{'file_name': row.externalIdentifier,
                                   'transcription': {'$elemMatch': {'transcriberID': row.transcriberID}}}})
    # ,'transcription': {'$elemMatch': {'transcriberID': row.transcriberID}}


# For each row in the csv file
for row in data.itertuples():
    content = db.imagevalidations.find_one({'file_name': row.externalIdentifier})
    if content is None:
        doc = db.imagevalidations.insert_one(
            {'file_name': row.externalIdentifier,
             'site_code': "AH1",
             'site_name': "AH1",
             'camera_name': _camera_name(row.externalIdentifier),
             'image_capture_date': _date_time(row.externalIdentifier),
             'expedition_name': "KI Dunnart Survey: 5 Western River & Borda Region - February 2020",
             'expedition_number': 76962823,
             'program_name': "KI Wildlife Recovery",
             'program_id': "P001"}
        )
        # print('inserted: ', doc)

    if not pd.isnull(row.dateTranscribed):
        db.imagevalidations.update_one({'file_name': row.externalIdentifier},
                                       {'$addToSet': {'transcriptions': {'$each': [{
                                           'transcriber': row.transcriberID,
                                           'comment': row.exportComment,
                                           'date': _to_dateobject(str(row.dateTranscribed), '%d/%m/%Y %H:%M'),
                                           "no_animals": 'False' if pd.isnull(row.noAnimalsVisible)
                                           else row.noAnimalsVisible,
                                           "any_problem_with_image": 'False' if pd.isnull(row.problemWithImage)
                                           else row.problemWithImage,
                                           'notes': row.transcriberNotes,
                                           'phase': '1PV',
                                           'observations': [
                                               {
                                                   'scientific_name': row.scientificName_0,
                                                   'vernacular_name': row.vernacularName_0,
                                                   'count': row.individualCount_0
                                               }
                                           ]
                                       }]}}})

        # db.imagevalidations.update_one({"file_name": row.externalIdentifier},
        #                                {'$addToSet': {'transcriptions':
        #                                                   {'$each': [{'age': 10, 'class': 2},
        #                                                              {'age': 22, 'class': 8}]}}})

    else:
        db.imagevalidations.update_one({'file_name': row.externalIdentifier},
                                       {'$addToSet': {'validations': {'$each': [{
                                           "validator": row.validatorID,
                                           "comment": row.exportComment,
                                           "date": _to_dateobject(str(row.dateValidated), '%d/%m/%Y %H:%M'),
                                           "no_animals": 'False' if pd.isnull(row.noAnimalsVisible)
                                           else row.noAnimalsVisible,
                                           "any_problem_with_image": 'False' if pd.isnull(row.problemWithImage)
                                           else row.problemWithImage,
                                           "notes": row.transcriberNotes,
                                           "phase": "1PV",
                                           "observations": [
                                               {
                                                   "scientific_name": row.scientificName_0,
                                                   "vernacular_name": row.vernacularName_0,
                                                   "count": row.individualCount_0,
                                                   "comments": row.validatorNotes
                                               }
                                           ]
                                       }]}}})

        # db.imagevalidations.update_one({"file_name": row.externalIdentifier},
        #                                {'$set': {'test_v': "testing validations!!"}},
        #                                upsert=True)

    # print(row.externalIdentifier)

# SADEW_KIFR_SR_Oct20_DA1B_20200925093636_SYER0001.JPG
# data = pd.read_excel(
#     "file:///C:\\Users\\noliyath\\workspace\\KI_CameraTraps"
#     "\\FAUNA_ALA_KI_Fire_DigiVol_Download_Append_V20210413_EXTRACT.xlsx", sheet_name='Sheet1', engine='openpyxl')
