// Task 1iv

db.ratings.aggregate([
    // Match documents with userId 186
    {
        $match: {
            userId: 186
        }
    },
    // Sort the matched documents by timestamp in descending order
    {
        $sort: {
            timestamp: -1
        }
    },
    // Limit the result set to the top 5 documents
    {
        $limit: 5
    },
    // Create new fields for movieIds, ratings, and timestamps
    {
        $addFields: {
            movieIds: "$movieId",
            ratings: "$rating",
            timestamps: "$timestamp"
        }
    },
    // Group documents and create arrays of movieIds, ratings, and timestamps
    {
        $group: {
            _id: 0,
            movieIds: { $push: "$movieIds" },
            ratings: { $push: "$ratings" },
            timestamps: { $push: "$timestamps" }
        }
    },
    // Project the desired fields: movieIds, ratings, and timestamps; Exclude the _id field
    {
        $project: {
            _id: 0,
            movieIds: 1,
            ratings: 1,
            timestamps: 1
        }
    }
]);
