// Task 2i

db.movies_metadata.aggregate([
    // Create a new field "score" based on the weighted rating formula
    {
        $addFields: {
            score: {
                $round: [
                    {
                        $add: [
                            {
                                $multiply: [
                                    {
                                        $divide: [
                                            "$vote_count",
                                            {
                                                $add: ["$vote_count", 1838]
                                            }
                                        ]
                                    },
                                    "$vote_average"
                                ]
                            },
                            {
                                $multiply: [
                                    {
                                        $divide: [
                                            1838,
                                            {
                                                $add: ["$vote_count", 1838]
                                            }
                                        ]
                                    },
                                    7
                                ]
                            }
                        ]
                    },
                    2
                ]
            }
        }
    },
    // Project the desired fields: title, vote_count, and score; Exclude the _id field
    {
        $project: {
            _id: 0,
            title: 1,
            vote_count: 1,
            score: 1
        }
    },
    // Sort the documents by score (descending), vote_count (descending), and title (ascending)
    {
        $sort: {
            score: -1,
            vote_count: -1,
            title: 1
        }
    },
    // Limit the result set to the top 20 documents
    {
        $limit: 20
    }
]);
