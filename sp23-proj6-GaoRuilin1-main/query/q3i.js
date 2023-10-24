db.credits.aggregate([
    // Stage 1: Unwind the cast array to create a document for each cast member
    { $unwind: "$cast" },
    
    // Stage 2: Filter the documents by cast member with id 7624
    { $match: { "cast.id": 7624 } },
    
    // Stage 3: Perform a lookup to join the movies_metadata collection based on movieId
    {
      $lookup: {
        from: "movies_metadata",
        localField: "movieId",
        foreignField: "movieId",
        as: "metadata"
      }
    },
    
    // Stage 4: Add new fields for film title, release date, and character role
    {
      $addFields: {
        title: { $arrayElemAt: ["$metadata.title", 0] },
        release_date: { $arrayElemAt: ["$metadata.release_date", 0] },
        character: "$cast.character"
      }
    },
    
    // Stage 5: Project the desired fields and exclude the _id field
    {
      $project: {
        _id: 0,
        title: 1,
        release_date: 1,
        character: 1
      }
    },
    
    // Stage 6: Sort the results by release date in descending order
    { $sort: { release_date: -1 } }
  ]);
  