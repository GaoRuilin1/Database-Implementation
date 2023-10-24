// Task 2iii

db.movies_metadata.aggregate([
    // Calculate the revised budget, round it to the nearest million, and create a new field
    {
        $addFields: {
            revise_budget: {
                $cond: [
                    {
                        $and: [
                            { $ne: ["$budget", ""] },
                            { $ne: ["$budget", undefined] },
                            { $ne: ["$budget", false] },
                            { $ne: ["$budget", null] }
                        ]
                    },
                    {
                        $round: [
                            {
                                $toInt: {
                                    $trim: {
                                        input: {
                                            $cond: [
                                                { $isNumber: "$budget" },
                                                { $toString: "$budget" },
                                                "$budget"
                                            ]
                                        },
                                        chars: " USD\$"
                                    }
                                }
                            },
                            -7
                        ]
                    },
                    "unknown"
                ]
            }
        }
    },
    // Group by the revised budget and count the occurrences
    {
        $group: {
            _id: "$revise_budget",
            count: { $sum: 1 }
        }
    },
    // Project the desired fields: budget and count; Exclude the _id field
    {
        $project: {
            _id: 0,
            budget: "$_id",
            count: 1
        }
    },
    // Sort the results by budget in ascending order
    {
        $sort: {
            budget: 1
        }
    }
]);
