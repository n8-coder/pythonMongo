from pymongo import MongoClient

# Requires the PyMongo package.
# https://api.mongodb.com/python/current

client = MongoClient('mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&ssl=false')
filter = {
    'validations.no_animals': 'False',
    'validations.observations.scientific_name': {
        '$ne': 'Homo sapiens'
    }
}

result = client['biodiversity']['imagevalidations'].find(
    filter=filter
)
