import test from 'ava'
import sql from 'sql-template-strings'
import { createQuery } from '../src'

test('insert single value', t => {
  const query = createQuery({
    kind: 'insert',
    table: 'people',
    values: [
      {
        name: 'Chris',
        female: false
      }
    ]
  })

  t.deepEqual(
    query,
    sql`insert into people (name, female) values (${'Chris'}, ${false})`
  )
})

test('insert single value with aliases', t => {
  const people = {
    name: 'people',
    aliases: {
      isFemale: 'female'
    }
  }

  const query = createQuery({
    kind: 'insert',
    table: people,
    values: [
      {
        name: 'Chris',
        isFemale: false
      }
    ]
  })

  t.deepEqual(
    query,
    sql`insert into people (name, female) values (${'Chris'}, ${false})`
  )
})

test('insert single value with returning', t => {
  const query = createQuery({
    kind: 'insert',
    table: 'people',
    values: [
      {
        name: 'Chris',
        female: false
      }
    ],
    returning: ['name', 'female']
  })

  t.deepEqual(
    query,
    sql`insert into people (name, female) values (${'Chris'}, ${false}) returning name, female`
  )
})

test('insert single value with returning with aliases', t => {
  const people = {
    name: 'people',
    aliases: {
      isFemale: 'female'
    }
  }

  const query = createQuery({
    kind: 'insert',
    table: people,
    values: [
      {
        name: 'Chris',
        isFemale: false
      }
    ],
    returning: ['name', 'isFemale']
  })

  t.deepEqual(
    query,
    sql`insert into people (name, female) values (${'Chris'}, ${false}) returning name, female as "isFemale"`
  )
})

test('insert multiple values', t => {
  const query = createQuery({
    kind: 'insert',
    table: 'people',
    values: [
      {
        name: 'Chris',
        female: false
      },
      {
        name: 'Chris 2',
        female: true
      }
    ],
    returning: ['name', 'female']
  })

  t.deepEqual(
    query,
    sql`insert into people (name, female) values (${'Chris'}, ${false}), (${'Chris 2'}, ${true}) returning name, female`
  )
})

test('select single column', t => {
  const query = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name']
  })

  t.deepEqual(query, sql`select name from people`)
})

test('select multiple columns', t => {
  const query = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name', 'female']
  })

  t.deepEqual(query, sql`select name, female from people`)
})

test('select with simple where', t => {
  const query = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name'],
    where: {
      female: ['=', false]
    }
  })

  t.deepEqual(query, sql`select name from people where female = ${false}`)
})

test('select with where and', t => {
  const query = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name'],
    where: {
      female: ['=', false],
      color: ['!=', 'blue']
    }
  })

  t.deepEqual(
    query,
    sql`select name from people where female = ${false} and color != ${'blue'}`
  )
})

test('select with where or', t => {
  const query = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name'],
    where: [
      {
        female: ['=', false]
      },
      {
        color: ['!=', 'blue']
      }
    ]
  })

  t.deepEqual(
    query,
    sql`select name from people where female = ${false} or color != ${'blue'}`
  )
})

test('select with where complex 1 -- a && (b || c)', t => {
  const query = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name'],
    where: [
      { female: ['=', false] },
      [{ color: ['=', 'blue'] }, { color: ['=', 'green'] }]
    ]
  })

  t.deepEqual(
    query,
    sql`select name from people where female = ${false} and (color = ${'blue'} or color = ${'green'})`
  )
})

test('select with where complex 1 -- a || (b && c) || d', t => {
  const query = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name'],
    where: [
      { female: ['=', false] },
      { color: ['=', 'blue'], female: ['=', true] },
      { color: ['=', 'green'] }
    ]
  })

  t.deepEqual(
    query,
    sql`select name from people where female = ${false} or (color = ${'blue'} and female = ${true}) or color = ${'green'}`
  )
})

test('select with where and order by', t => {
  const query = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name'],
    where: {
      color: ['=', 'blue']
    },
    orderBy: [
      {
        column: 'name',
        sort: 'asc'
      }
    ]
  })

  t.deepEqual(
    query,
    sql`select name from people where color = ${'blue'} order by name asc`
  )
})

test('update with where', t => {
  const query = createQuery({
    kind: 'update',
    table: 'people',
    set: {
      color: 'green'
    },
    where: {
      color: ['=', 'blue']
    }
  })

  t.deepEqual(
    query,
    sql`update people set color = ${'green'} where color = ${'blue'}`
  )
})

test('delete with where', t => {
  const query = createQuery({
    kind: 'delete',
    table: 'people',
    where: {
      color: ['=', 'blue']
    }
  })

  t.deepEqual(query, sql`delete from people where color = ${'blue'}`)
})

test('delete with where is null', t => {
  const query = createQuery({
    kind: 'delete',
    table: 'people',
    where: {
      color: ['=', null]
    }
  })

  t.deepEqual(query, sql`delete from people where color is null`)
})

test('delete with where is not null', t => {
  const query = createQuery({
    kind: 'delete',
    table: 'people',
    where: {
      color: ['!=', null]
    }
  })

  t.deepEqual(query, sql`delete from people where color is not null`)
})

function stitch (statement) {
  let str = ''
  statement.strings.forEach((string, index) => {
    let val = statement.values[index]
    if (typeof val === 'string') {
      val = `'${val}'`
    }
    str += string + (val === undefined ? '' : val)
  })
  return str
}

test('readme examples', t => {
  const bluePeopleInsert = createQuery({
    kind: 'insert',
    table: 'people',
    values: [{ name: 'Chris', color: 'blue' }, { name: 'Kevin', color: 'blue' }]
  })
  t.deepEqual(
    stitch(bluePeopleInsert),
    "insert into people (name, color) values ('Chris', 'blue'), ('Kevin', 'blue')"
  )

  const bluePeopleSelect = createQuery({
    kind: 'select',
    table: 'people',
    columns: ['name', 'age'],
    where: {
      color: ['=', 'blue']
    }
  })
  t.deepEqual(
    stitch(bluePeopleSelect),
    "select name, age from people where color = 'blue'"
  )

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
  t.deepEqual(
    stitch(bluePeopleUpdate),
    "update people set color = 'green' where color = 'blue'"
  )
})
