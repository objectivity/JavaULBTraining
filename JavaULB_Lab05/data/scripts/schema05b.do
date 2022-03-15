

UPDATE SCHEMA {
	CREATE CLASS Person {
		FirstName	: STRING,
		MiddleName	: STRING,
		LastName	: STRING,
		
		LivesAt		: LIST {
						element: Reference {
							edgeClass		: LivesAtEdge,
							edgeAttribute	: ToAddress
						},
						CollectionTypeName	: SegmentedArray
					}
    }
	
	CREATE CLASS Address
    {
		Street	: STRING,
		City	: STRING,
		State	: STRING,
		ZIP		: STRING,
		Latitude : REAL {STORAGE: B32},
		Longitude : REAL {STORAGE: B32},
        LivesHere
              : List {
                  element: Reference {
                      edgeClass		: LivesAtEdge,
                      edgeAttribute	: ToPerson
                   },
                   CollectionTypeName	: SegmentedArray
                }
    }
	
    CREATE CLASS LivesAtEdge
    {
       ToAddress 		: Reference {referenced: Address, inverse: LivesHere },
       ToPerson			: Reference {referenced: Person,  inverse: LivesAt }
    }
};