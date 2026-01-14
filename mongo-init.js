// Initialize MongoDB for the weather pipeline.
db = db.getSiblingDB("big_data_austria");
db.createCollection("raw");
db.createCollection("processed");
