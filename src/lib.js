const { Base64 } = require('js-base64')
const config = require('./config')

/**
 * Implementation of the Relay-style cursor pagination specification for Sequelize + PostgresQL
 * Spec: https://relay.dev/graphql/connections.htm
 *
 * @param {object} args - relay connection query arguments
 * Sort arguments:
 *      @param {string} args.sort - field name to sort by
 *      @param {string} args.direction - 'ASC' or 'DESC'
 * [Forward pagination arguments]:
 *      @param {number} [args.first] - page length, non‐negative integer
 *      @param {string} [args.after] - base64 encoded cursor
 * [Backward pagination arguments]:
 *      @param {number} [args.last] - page length, non‐negative integer
 *      @param {string} [args.before] - base64 encoded cursor
 * @param {object} [args.search] - contains searchTerm to search for and columns to search in
 * @param {object} [model] - SequelizeCache instance containing model to query
 * @param {object} [fieldMap] - mapping of graphQL field names to their respective DB column names
 * @param {object} [customQuery] - custom query object for queries w/o sequelize models
 *      @param {object} customQuery.db - SequelizeCache-QueryCache instance
 *      @param {string} customQuery.queryString - raw SQL query (requires 'count(*) OVER() AS full_count' for
 *          pagination calculation, see AssetMarket/resolvers.js for an example)
 *      @param {string} customQuery.replacements - sequelize replacements array, ex: ['bitcoin', 'bitcoin'] replaces ?
 *           in queryString
 * @returns {object} relay connection
 */
const createConnection = async (
  { after, before, direction, first, last, search, sort, where = {} },
  model,
  fieldMap,
  customQuery
) => {
  validate({ after, before, direction, fieldMap, sort, first, last })

  const { order, flip } = effectiveOrder({
    direction,
    fieldMap,
    last,
    sort
  })

  let limit = flip ? last : first
  const cursor = flip ? before : after
  where = search ? addSearchExpression(where, search, fieldMap) : where
  const offset = await getOffset(cursor, flip, where, model, customQuery)
  limit = limit || config.defaultLimit || 100

  if (customQuery) {
    const queryAddOns = ` ORDER BY ${order[0][0]} ${order[0][1]} LIMIT ? OFFSET ?;`

    return customQuery.db
      .query(customQuery.queryString.concat(queryAddOns), {
        replacements: customQuery.replacements.concat([limit + 1, offset]) // +1 to peek to see if there is more data
      })
      .then(results => {
        const queryResults = {
          rows: results,
          count: results.length ? results[0].full_count : 0
        }
        return buildConnection(queryResults, limit, fieldMap, flip, offset, order)
      })
  }
  return model
    .findAndCountAll({
      limit: limit + 1, // +1 to peek to see if there is more data
      offset,
      order,
      where
    })
    .then(queryResults => buildConnection(queryResults, limit, fieldMap, flip, offset, order))
}

const buildConnection = (queryResults, limit, fieldMap, flip, offset, order) => {
  const hasMoreResults = queryResults.rows.length === limit + 1
  let results = hasMoreResults ? queryResults.rows.slice(0, -1) : queryResults.rows
  results = flip ? results.reverse() : results
  results = fieldMap ? mapDbFieldsToGraphQlFields(results, fieldMap) : results
  results = convertDateToUnix(results)

  return {
    edges: getEdges(results, flip, offset, queryResults.count),
    pageInfo: getPageInfo({
      count: queryResults.count,
      flip,
      hasMoreResults,
      offset,
      order,
      results
    }),
    totalCount: results.length
  }
}

const validate = args => {
  if (!args.first && !args.last) {
    throw new Error(
      'Validation error: Arguments `first` or `last` are required to properly paginate the connection.'
    )
  } else if (args.after && args.before) {
    throw new Error('Validation error: Arguments after and before must not be together')
  } else if (!args.sort || !args.direction) {
    throw new Error('Validation error: Arguments sort and direction are required')
  } else if (typeof args.fieldMap !== 'object') {
    throw new Error('Validation error: Argument fieldMap is required')
  } else if (args.first > config.maxLimit || args.last > config.maxLimit) {
    throw new Error(`Validation error: Max limit for first and last is ${config.maxLimit}`)
  } else if (args.first < 1 || args.last < 1) {
    throw new Error('Validation error: First and last must be greater than 1')
  }
}
/**
 *
 * @param {string} cursor - base64 encoded cursor
 * @param {boolean} flip - flag for backwards pagination
 * @param {object} where - sequelize where object
 * @param [model] - required if querying w/ SequelizeCache instance
 * @param [customQuery] - required if querying w/ customQuery
 * @returns {Promise} Promise object represents the offset (number)
 */
const getOffset = async (cursor, flip, where, model, customQuery) => {
  if (cursor) {
    const decodedCursor = parseInt(Base64.atob(cursor))

    if (isNaN(decodedCursor)) {
      throw new Error('Validation error: Invalid cursor')
    }

    if (!flip) {
      return decodedCursor
    }

    let count

    if (customQuery) {
      count = await customQuery.db
        .query(customQuery.queryString, {
          replacements: customQuery.replacements
        })
        .then(results => (results[0].length ? results[0][0].full_count : 0))
    } else {
      count = await model.count({
        where
      })
    }

    let offset = count - decodedCursor + 1
    if (offset < 0) {
      throw new Error('Validation error: Invalid cursor')
    }
    return offset
  }
  return 0
}

const getEdges = (results, flip, offset, count) =>
  results.map((result, index) => ({
    cursor: getCursor(offset, index, flip, count, results),
    node: result
  }))

/**
 * Determines order for the SQL query
 * @param {string} sort - field name to sort by
 * @param {string} direction - 'ASC' or 'DESC'
 * @param {number} first - page length, non‐negative integer
 * @param {number} last - page length, non‐negative integer
 * @returns {object} Contains sequelize 'order' array and flip flag for pagination logic
 */
const effectiveOrder = ({ direction, last, sort, fieldMap }) => {
  const order = [[fieldMap[sort], `${direction} NULLS LAST`]]
  // flips `ASC` to `DESC` (and vice-versa) if pagination arg `last` is defined
  if (last) {
    return {
      flip: true,
      order: [[order[0][0], flipSortDirection(order[0][1])]]
    }
  }
  return { flip: false, order }
}

/**
 * Creates an opaque cursor based on the position of the result in the results array
 * @param {number} offset - integer offset
 * @param {array} index - position of result in results array
 * @param {boolean} flip - flag for backwards pagination
 * @param {number} count - integer count of total DB results for query (before search or limit applied)
 * @param {array} results - actual DB query results with search and limit applied
 * @returns {string} A base64 encoded string representing a db value
 */
const getCursor = (offset, index, flip, count, results) => {
  const position = flip ? count - offset - results.length + index + 1 : offset + index + 1
  return Base64.btoa(position.toString())
}

/**
 * Adds to a sequelize 'where' object to filter the query by supplied search term and column(s)
 * @param {object} where - existing sequelize 'where' object to add to
 * @param {object} search - search object containing searchTerm and columns
 * @param {string} search.searchTerm - search parameter, e.g. 'bitcoin'
 * @param {array} search.columns - array of {string} column names to search, e.g. ['symbol', 'display_name']
 * @param {object} fieldMap - used to convert graphQL key name to DB key name
 * @returns {object} sequelize 'where' object
 */
const addSearchExpression = (where, { searchTerm, columns }, fieldMap) => ({
  ...where,
  $or: columns.map(field => ({
    [fieldMap[field]]: {
      $iLike: `${searchTerm}%`
    }
  }))
})

/**
 * Reverses sort direction for backwards pagination
 * @param {string} sortDirection - e.g. 'DESC'
 * @returns {string} Order direction for SQL e.g. 'ASC'
 */
const flipSortDirection = sortDirection =>
  sortDirection === 'ASC NULLS LAST' ? 'DESC NULLS LAST' : 'ASC NULLS LAST'

/**
 * Builds a relay pageInfo field
 * @param {array} results - query results from db
 * @param {boolean} hasMoreResults - if there is another page
 * @param {boolean} flip - flag to indicate backwards pagination
 * @param {number} offset - the offset from DB query
 * @param {number} count - integer count of total DB results for query (before search or limit applied)
 * @returns {object} A relay pageInfo field
 */
const getPageInfo = ({ results, hasMoreResults, flip, offset, count }) => {
  const startCursor = results.length ? getEdges(results, flip, offset, count)[0].cursor : null
  const endCursor = results.length
    ? getEdges(results, flip, offset, count).slice(-1)[0].cursor
    : null

  return {
    endCursor,
    hasNextPage: flip ? false : hasMoreResults,
    hasPreviousPage: flip ? hasMoreResults : false,
    startCursor
  }
}

/**
 * Renames DB keys to match graphQL keys and drops any extra fields
 * from the DB data not present in the map
 * @param {array} results - DB query results
 * @param {object} map - a graphQL to DB key name mapping
 * @returns {array} - Re-keyed list of results to match graphQL schema
 */
const mapDbFieldsToGraphQlFields = (results, map) =>
  results.map(result =>
    Object.keys(map).reduce(
      (acc, key) => ({
        [key]: result[map[key]],
        ...acc
      }),
      {}
    )
  )

const convertDateToUnix = results =>
  results.map(result => {
    try {
      if (result.updatedAt) {
        result.updatedAt = result.updatedAt.valueOf()
      }
      if (result.createdAt) {
        result.createdAt = result.createdAt.valueOf()
      }
      if (result.deletedAt) {
        result.deletedAt = result.deletedAt.valueOf()
      }
      return result
    } catch (error) {
      logger.error('Error parsing date:', error)
      return result
    }
  })

/**
 * @deprecated
 * Sets a default sort/direction if not present in connector args and
 * specifies columns that the search should be applied to if
 * a search is passed in
 * @param {object} args - arguments passed to connector
 * @param {string} [args.search] - search term
 * @param {string} [args.sort] - column to sort by
 * @param {string} [args.direction] - 'ASC' or 'DESC'
 * @param {array} searchColumns - column(s) to apply search to e.g ['displayName', 'symbol']
 * @param {string} defaultDirection - default direction for when direction not present in args e.g. 'ASC' or 'DESC'
 * @param {string} defaultSort - default sort for when sort not present in args e.g 'marketCapUsd'
 * @param {object} [customWhere] - custom where object for extra filtering i.e. { exchange_id: 'binance' }
 * @returns {object} - enriched args
 */
const enrichArgs = ({ args, searchColumns, defaultDirection, defaultSort, customWhere }) => ({
  ...args,
  direction: args.direction || defaultDirection,
  search: args.search
    ? {
        columns: searchColumns,
        searchTerm: args.search
      }
    : undefined,
  sort: args.sort || defaultSort,
  where: customWhere || {}
})

const handleError = err => {
  if (err.message.includes('Validation error:')) {
    throw err
  } else {
    throw new Error('An unexpected error has occurred. Please try back again later.')
  }
}

const getUserId = ctx => {
  if (ctx.request) {
    const Authorization = ctx.request.get('Authorization')
    if (Authorization) {
      const id = Authorization.replace('Bearer ', '')
      return id || null
    }
  }
  return null
}

const isFeatureEnabled = async (ctx, key) => {
  await ctx.ldClient.waitForInitialization()
  return ctx.ldClient.variation(key, ctx.ldUser, false)
}

/**
 * Calls sequelize's destroy on the given model for a given where criteria and returns the deleted item
 * @param model - sequelize model
 * @param where - sequelize where object
 * @returns {Promise<Object>}
 */
const deleteAndReturn = async (model, where) => {
  const result = await model.findOne({ where })
  const deleteCount = await model.destroy({ where })

  if (deleteCount) {
    return result
  } else {
    throw new Error('Validation Error: Delete failed!')
  }
}

module.exports = {
  createConnection,
  convertDateToUnix,
  deleteAndReturn,
  enrichArgs,
  getUserId,
  handleError,
  isFeatureEnabled,
  mapDbFieldsToGraphQlFields
}
