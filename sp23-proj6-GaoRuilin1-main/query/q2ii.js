// Task 2ii

db.movies_metadata.aggregate([
    // Split the tagline field into words, remove punctuation marks, and convert to lowercase
    {
        $project: {
            words: {
                $map: {
                    input: { $split: ["$tagline", " "] },
                    as: "word",
                    in: {
                        $toLower: {
                            $trim: {
                                input: "$$word",
                                chars: ".,!?"
                            }
                        }
                    }
                }
            }
        }
    },
    // Unwind the array of words
    {
        $unwind: "$words"
    },
    // Calculate the length of the words
    {
        $addFields: {
            len: { $strLenCP: "$words" }
        }
    },
    // Filter out words shorter than 4 characters
    {
        $match: {
            len: { $gt: 3 }
        }
    },
    // Group by lowercase words and count their occurrences
    {
        $group: {
            _id: "$words",
            count: { $sum: 1 }
        }
    },
    // Sort the results by count in descending order and then by word in ascending order
    {
        $sort: {
            count: -1,
            _id: 1
        }
    },
    // Limit the result set to the top 20 words
    {
        $limit: 20
    }
]);
