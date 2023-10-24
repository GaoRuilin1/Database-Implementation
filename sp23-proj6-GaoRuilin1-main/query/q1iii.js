// Task 1iii

db.ratings.aggregate([
    // Group documents by the "rating" field and count the number of occurrences for each rating
    {
        $group: {
            _id: "$rating",
            count: { $sum: 1 }
        }
    },
    // Sort the documents by rating in descending order
    {
        $sort: {
            _id: -1
        }
    },
    // Project the desired fields: rating and count; Exclude the _id field
    {
        $project: {
            _id: 0,
            rating: "$_id",
            count: 1
        }
    }
]);
