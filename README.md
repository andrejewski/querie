# Querie
> SQL query builder

Construct queries to pass to [`node-postgres`](https://github.com/brianc/node-postgres) [`query()`](https://node-postgres.com/features/queries).

This library uses [`sql-template-strings`](https://github.com/felixfbecker/node-sql-template-strings) to build query statement objects.

## Example

```js
import { createQuery } from 'querie'

const bluePeopleInsert = createQuery({
  kind: 'insert',
  table: 'people',
  values: [
    { name: 'Chris', color: 'blue' },
    { name: 'Kevin', color: 'blue' }
  ]
})
// => insert into people (name, color) values ('Chris', 'blue'), ('Kevin', 'blue')

const bluePeopleSelect = createQuery({
  kind: 'select',
  table: 'people',
  columns: ['name', 'age'],
  where: {
    color: ['=', 'blue']
  }
})
// => select name, age from people where color = 'blue'

const bluePeopleUpdate = createQuery({
  kind: 'update',
  table: 'people',
  set: {
    color: 'green'
  },
  where: {
    color: ['=', 'blue']
  }
})
// => update people set color = 'green' where color = 'blue'
```

## Features

- **State and side-effect free query building.** <br>
  Queries are built without knowledge of or connection to a database.

- **Data driven queries** <br>
  Queries are built using plain JavaScript data structures, no method chaining.

- **Column aliases** <br>
  Alias JS friendly column names to what is in the database seamlessly.

