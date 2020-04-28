

UPDATE SCHEMA {
	CREATE CLASS Person {
		FirstName	: STRING,
		MiddleName	: STRING,
		LastName	: STRING,
		
		LivesAt		: LIST {
						element: Reference {
							edgeClass		: LivesAt,
							edgeAttribute	: LivesAt
						},
						CollectionTypeName	: SegmentedArray
					}
    }
	
	CREATE CLASS AAddress
    {
		Street	: STRING,
		City	: STRING,
		State	: STRING,
        LivesHere
              : List {
                  element: Reference {
                      edgeClass		: LivesAt,
                      edgeAttribute	: LivesHere
                   },
                   CollectionTypeName	: SegmentedArray
                }
    }
	
    CREATE CLASS LivesAt
    {
       LivesAt 		: Reference {referenced: AAddress, inverse: LivesHere },
       LivesHere	: Reference {referenced: APerson, inverse: LivesAt }
    }
};