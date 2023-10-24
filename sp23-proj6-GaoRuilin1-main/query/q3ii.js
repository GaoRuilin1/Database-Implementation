// Task 3ii

db.credits.aggregate([
    // Filter documents to only those where the crew array contains a director with id 5655
    {
        $match: {
            crew: { $elemMatch: { id: 5655, job: "Director" } }
        }
    },
    // Separate the documents for each cast member
    { $unwind: "$cast" },
    // Retain only the necessary fields: id and name
    {
        $project: {
            actorId: "$cast.id",
            actorName: "$cast.name",
            _id: 0
        }
    },
    // Group documents by actor id and name, and count their occurrences
    {
        $group: {
            _id: { name: "$actorName", id: "$actorId" },
            movieCount: { $sum: 1 }
        }
    },
    // Format the output fields
    {
        $project: {
            _id: 0,
            id: "$_id.id",
            name: "$_id.name",
            count: "$movieCount"
        }
    },
    // Sort the results by count (descending) and id (ascending)
    { $sort: { count: -1, id: 1 } },
    // Limit the output to the top 5 results
    { $limit: 5 }
]);
