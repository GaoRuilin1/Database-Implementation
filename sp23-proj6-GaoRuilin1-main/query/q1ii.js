// Task 1ii

db.movies_metadata.aggregate([
    // Match documents with "Comedy" genre and at least 50 votes
    {
        $match: {
            $and: [
                { genres: { $elemMatch: { name: "Comedy" } } },
                { vote_count: { $gte: 50 } }
            ]
        }
    },
    // Sort the matched documents by vote_average (descending), vote_count (descending), and movieId (ascending)
    {
        $sort: {
            vote_average: -1,
            vote_count: -1,
            movieId: 1
        }
    },
    // Limit the result set to the top 50 documents
    {
        $limit: 50
    },
    // Project only the desired fields: title, vote_average, vote_count, and movieId; Exclude the _id field
    {
        $project: {
            _id: 0,
            title: 1,
            vote_average: 1,
            vote_count: 1,
            movieId: 1
        }
    }
]);
