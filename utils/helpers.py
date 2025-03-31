from pymongo import MongoClient
client = MongoClient("mongodb://localhost:27017")
collection = client["entertainment"]["films"]

genres = ["Drame"]
actor = "Casey Affleck"

query = {
    "genre": {"$in": genres},
    "Actors": {"$not": {"$regex": actor, "$options": "i"}},
    "rating": {"$gte": 5.0},
    "Votes": {"$gte": 500}
}

result = collection.find_one(query, {"title": 1, "genre": 1, "Actors": 1, "rating": 1, "Votes": 1})
print(result)
