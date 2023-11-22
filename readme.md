## Cursus Database System
JSON based, unstructured distributed database.  Capable of extremely fast parallel search.

## Query Language
### Selecting
```
select * from yourCollection where key == 'value';
select * from yourCollection where key != 'value';
select * from yourCollection where key != false;
select * from yourCollection where key >= 1;
select * from yourCollection where key <= 1;
select * from yourCollection where key < 1;
select * from yourCollection where key > 1.55;

```

You can slice and limit

Limiting
``` 
select 4 from yourCollection where key == 'value';
```

Skipping 4 from first in collection
``` 
select 4,* from yourCollection where key == 'value';
```

Skipping 4 from first in collection and getting 2 documents
``` 
select 4,6 from yourCollection where key == 'value';
```

### Actions
- select
- update
- delete

## Todo
- && queries (``select * from users where firstName == 'Alex' && age > 21;``)
- Finish delete
- Comments
- Encryption at rest
- Each node should have a limit of documents. Say 10 million default per node.