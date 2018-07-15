const sql = require('sql-template-strings')
const { ident: escapeLowerCaseIdentifier } = require('pg-escape')

function escapeIdentifier (identifier) {
  const escapedIdentifier = escapeLowerCaseIdentifier(identifier)
  const isSensitiveAndNotEscaped =
    identifier !== identifier.toLowerCase() && identifier === escapedIdentifier

  return isSensitiveAndNotEscaped ? `"${escapedIdentifier}"` : escapedIdentifier
}

function getTableName (table) {
  return typeof table === 'string' ? table : table.name
}

function getTableColumnName (table, columnName) {
  if (table && table.aliases) {
    while (table.aliases[columnName]) {
      columnName = table.aliases[columnName]
    }
  }
  return escapeIdentifier(columnName)
}

function sqlConcat (frags) {
  return frags.reduce((build, part) => build.append(part))
}

function sqlJoin (frags, separator = ', ') {
  return frags.reduce((sql, frag) => sql.append(separator).append(frag))
}

function sqlValueList (items) {
  return sqlConcat(items.map((e, i) => (i === 0 ? sql`${e}` : sql`, ${e}`)))
}

function sqlWrappedValueList (items) {
  return sql`(`.append(sqlValueList(items)).append(')')
}

function getUniqueKeys (objects) {
  const keyDict = {}
  objects.forEach(object => {
    Object.keys(object).forEach(key => {
      keyDict[key] = true
    })
  })

  return Object.keys(keyDict)
}

function insert ({ table, columns, values, returning }) {
  if (!(values && values.length)) {
    return null
  }

  const codeColumns = columns || getUniqueKeys(values)
  if (!codeColumns.length) {
    return null
  }

  const dbColumns = codeColumns.map(columnName =>
    getTableColumnName(table, columnName)
  )

  const dbDataRows = values.map(record =>
    codeColumns.map(columnName => record[columnName])
  )
  const dbListRows = dbDataRows.map(sqlWrappedValueList)
  const dbValues = sqlJoin(dbListRows, ', ')

  const baseQuery = sql`insert into `
    .append(
      `${escapeIdentifier(getTableName(table))} (${dbColumns.join(', ')}) values `
    )
    .append(dbValues)

  const returningClause = getReturningClause({ table, returning })
  return returningClause ? baseQuery.append(returningClause) : baseQuery
}

function getMappedColumns (table, columns) {
  return columns
    .map(columnName => {
      const dbColumnName = getTableColumnName(table, columnName)
      const codeColumnName = escapeIdentifier(columnName)
      return dbColumnName === codeColumnName
        ? dbColumnName
        : `${dbColumnName} as ${codeColumnName}`
    })
    .join(', ')
}

function getReturningClause ({ table, returning }) {
  if (!(returning && returning.length)) {
    return null
  }

  return ` returning ${getMappedColumns(table, returning)}`
}

function withWhereAndReturning (query, { table, where, returning }) {
  const whereClause = getWhereClause({ table, where })
  if (whereClause) {
    query = query.append(whereClause)
  }

  const returningClause = getReturningClause({ table, returning })
  if (returningClause) {
    query = query.append(returningClause)
  }

  return query
}

function update (options) {
  const { table, set } = options
  const entries = Object.entries(set)
  if (!entries.length) {
    return null
  }

  const columnList = []
  const valuesList = []
  entries.forEach(([key, value]) => {
    columnList.push(getTableColumnName(table, key))
    valuesList.push(value)
  })

  let leftHand, rightHand
  if (columnList.length > 1) {
    leftHand = `(${columnList.join(',')})`
    rightHand = sqlWrappedValueList(valuesList)
  } else {
    leftHand = columnList[0]
    rightHand = sql`${valuesList[0]}`
  }

  const query = sql`update `
    .append(`${getTableName(table)} set ${leftHand} = `)
    .append(rightHand)

  return withWhereAndReturning(query, options)
}

function _delete (options) {
  const { table } = options
  const query = sql`delete from `.append(`${getTableName(table)}`)
  return withWhereAndReturning(query, options)
}

function getOrderByClause ({ table, orderBy }) {
  if (!(orderBy && orderBy.length)) {
    return null
  }

  const orderClauses = orderBy.map(order => {
    const { column, sort, nulls } = order
    const dbColumnName = getTableColumnName(table, column)
    let clause = sql``.append(dbColumnName)

    if (sort) {
      if (sort === 'asc') {
        clause = clause.append(' asc')
      } else if (sort === 'desc') {
        clause = clause.append(' desc')
      } else {
        clause = clause.append(` using ${sort}`)
      }
    }

    if (nulls) {
      clause = clause.append(` nulls ${nulls}`)
    }

    return clause
  })

  return sql` order by `.append(sqlJoin(orderClauses))
}

function select (options) {
  const { table, columns, limit, offset } = options
  let query = sql`select `.append(
    `${getMappedColumns(table, columns)} from ${getTableName(table)}`
  )

  const orderByClause = getOrderByClause(options)
  if (orderByClause) {
    query = query.append(orderByClause)
  }

  const whereClause = getWhereClause(options)
  if (whereClause) {
    query = query.append(whereClause)
  }

  if (limit) {
    query = query.append(sql` limit ${limit}`)
  }

  if (offset) {
    query = query.append(sql` offset ${offset}`)
  }

  return query
}

function wrap (sqlFragment) {
  return sql`(`.append(sqlFragment).append(')')
}

function getAndConditions ({ table, and, _wrap }) {
  const andEntries = and ? Object.entries(and) : []
  return andEntries
    .filter(entry => {
      const hasValue = entry[1][1] !== undefined
      return hasValue
    })
    .map(([columnName, op]) => {
      const dbColumnName = getTableColumnName(table, columnName)
      const [operation, value, secondValue] = op

      if (value === null) {
        const isNull = operation === '='
        if (isNull) {
          return sql``.append(`${dbColumnName} is null`)
        }

        const isNotNull = operation === '!=' || operation === '<>'
        if (isNotNull) {
          return sql``.append(`${dbColumnName} is not null`)
        }
      }

      const lowOperation = operation.toLowerCase()
      if (lowOperation === 'between' || lowOperation === 'not between') {
        return sql``
          .append(`${dbColumnName} ${operation}`)
          .append(sql`${value} and ${secondValue}`)
      }

      return sql``.append(`${dbColumnName} ${operation} `).append(sql`${value}`)
    })
}

function getOrCondition ({ table, or }) {
  const hasConditions = Array.isArray(or) && or.length > 0
  if (!hasConditions) {
    return null
  }

  const orConditions = or
    .map(where => getWhereCondition({ table, where, _inner: true }))
    .filter(x => x)

  if (!orConditions.length) {
    return null
  }

  return sqlJoin(orConditions, ' or ')
}

function formatWhere (where) {
  return Array.isArray(where)
    ? Array.isArray(where[1]) ? where : [null, where]
    : [where]
}

function getWhereCondition ({ table, where, _inner }) {
  const [and, or] = formatWhere(where)
  const conditions = getAndConditions({ table, and })

  let orCondition = getOrCondition({ table, or })
  if (orCondition) {
    const shouldWrap = conditions.length
    if (shouldWrap) {
      orCondition = wrap(orCondition)
    }

    conditions.push(orCondition)
  }

  if (!conditions.length) {
    return null
  }

  const query = sqlJoin(conditions, ' and ')
  const shouldWrap = _inner && conditions.length > 1
  return shouldWrap ? wrap(query) : query
}

function getWhereClause (options) {
  const whereCondition = getWhereCondition(options)
  if (!whereCondition) {
    return null
  }

  return sql` where `.append(whereCondition)
}

const statementDict = {
  delete: _delete,
  insert,
  select,
  update
}

function createQuery (options) {
  return statementDict[options.kind](options)
}

exports.createQuery = createQuery
