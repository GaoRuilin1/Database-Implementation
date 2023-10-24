// Find the IDs of all movies labeled with the keyword "mickey mouse" or "marvel comic" by writing a query on the keywords collection. 
//Order your output in ascending order of movieId. 
//The output documents should have the following fields:

db.keywords.aggregate([
    // Match documents with either "mickey mouse" or "marvel comic" as keyword names
    {
        $match: {
            keywords: {
                $elemMatch: {
                    name: {
                        $in: ["mickey mouse", "marvel comic"] // Using $in to match either keyword name
                    }
                }
            }
        }
    },
    // Sort the matched documents by movieId in ascending order
    {
        $sort: {
            movieId: 1
        }
    },
    // Project only the desired fields, excluding _id and keywords
    {
        $project: {
            _id: 0,
            keywords: 0
        }
    }
]);
