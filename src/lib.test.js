const { Base64 } = require('js-base64')
const {
  createConnection,
  deleteAndReturn,
  enrichArgs,
  handleError,
  mapDbFieldsToGraphQlFields
} = require('./lib')
const { dbData, normalizedData } = require('../__mocks__/sampleAssetsData.js')
const assetsFieldMap = require('./schema/Asset/resolvers').fieldMap
const exchangesFieldMap = require('./schema/Exchange/resolvers').fieldMap
const config = require('./config')

describe('createConnection', () => {
  it('returns all data when sort and direction are supplied and first greater than db rows', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData
        })
      )
    }

    const actual = await createConnection(
      { direction: 'DESC', sort: 'marketCapUsd', first: 200 },
      mockModel,
      assetsFieldMap
    )

    expect(mockModel.findAndCountAll).toHaveBeenCalledWith({
      limit: 200 + 1,
      offset: 0,
      order: [['market_cap', 'DESC NULLS LAST']],
      where: {}
    })

    const expected = {
      edges: normalizedData.map((asset, index) => ({
        cursor: Base64.btoa((index + 1).toString()),
        node: asset
      })),
      pageInfo: {
        endCursor: Base64.btoa('12'),
        hasNextPage: false,
        hasPreviousPage: false,
        startCursor: Base64.btoa('1')
      },
      totalCount: 12
    }

    expect(actual).toEqual(expected)
  })

  it('throws an error when limit > config.maxLimit', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData.slice(0, 11)
        })
      )
    }

    await expect(
      createConnection(
        { direction: 'DESC', first: 9999, sort: 'marketCapUsd' },
        mockModel,
        assetsFieldMap
      )
    ).rejects.toThrowError(`Max limit for first and last is ${config.maxLimit}`)
  })

  it('throws an error when first < 1', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData.slice(0, 11)
        })
      )
    }

    await expect(
      createConnection(
        { direction: 'DESC', first: -1, sort: 'marketCapUsd' },
        mockModel,
        assetsFieldMap
      )
    ).rejects.toThrowError('First and last must be greater than 1')
  })

  it('throws an error when last < 1', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData.slice(0, 11)
        })
      )
    }

    await expect(
      createConnection(
        { direction: 'DESC', last: -1, sort: 'marketCapUsd' },
        mockModel,
        assetsFieldMap
      )
    ).rejects.toThrowError('First and last must be greater than 1')
  })

  it('returns correct data when first, sort, & direction are supplied', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData.slice(0, 11)
        })
      )
    }

    const actual = await createConnection(
      { direction: 'DESC', first: 10, sort: 'marketCapUsd' },
      mockModel,
      assetsFieldMap
    )

    expect(mockModel.findAndCountAll).toHaveBeenCalledWith({
      limit: 10 + 1,
      offset: 0,
      order: [['market_cap', 'DESC NULLS LAST']],
      where: {}
    })
    const expected = {
      edges: normalizedData
        .map((asset, index) => ({
          cursor: Base64.btoa((index + 1).toString()),
          node: asset
        }))
        .slice(0, 10),
      pageInfo: {
        endCursor: Base64.btoa('10'),
        hasNextPage: true,
        hasPreviousPage: false,
        startCursor: Base64.btoa('1')
      },
      totalCount: 10
    }
    expect(actual).toEqual(expected)
  })

  it('returns correct data when first, after, sort, & direction are supplied', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData.slice(4, 10)
        })
      )
    }

    const actual = await createConnection(
      {
        after: Base64.btoa('4'),
        direction: 'DESC',
        first: 5,
        sort: 'marketCapUsd'
      },
      mockModel,
      assetsFieldMap
    )

    expect(mockModel.findAndCountAll).toHaveBeenCalledWith({
      limit: 5 + 1,
      offset: 4,
      order: [['market_cap', 'DESC NULLS LAST']],
      where: {}
    })
    const expected = {
      edges: normalizedData.slice(4, 9).map((asset, index) => ({
        cursor: Base64.btoa((index + 4 + 1).toString()),
        node: asset
      })),
      pageInfo: {
        endCursor: Base64.btoa('9'),
        hasNextPage: true,
        hasPreviousPage: false,
        startCursor: Base64.btoa('5')
      },
      totalCount: 5
    }
    expect(actual).toEqual(expected)
  })

  it('returns correct data when first, after, sort and direction are supplied', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData.sort((a, b) => a.symbol.localeCompare(b.symbol)).slice(4, 10)
        })
      )
    }

    const actual = await createConnection(
      {
        after: Base64.btoa('4'), // ETH is 4th in this sort/direction
        direction: 'ASC',
        first: 5,
        sort: 'symbol'
      },
      mockModel,
      assetsFieldMap
    )
    expect(mockModel.findAndCountAll).toHaveBeenCalledWith({
      limit: 5 + 1,
      offset: 4,
      order: [['symbol', 'ASC NULLS LAST']],
      where: {}
    })
    const expected = {
      edges: normalizedData
        .sort((a, b) => a.symbol.localeCompare(b.symbol))
        .map((asset, index) => ({
          cursor: Base64.btoa((index + 1).toString()),
          node: asset
        }))
        .slice(4, 9),
      pageInfo: {
        endCursor: Base64.btoa('9'), // XLM is 9th in this sort/direction,
        hasNextPage: true,
        hasPreviousPage: false,
        startCursor: Base64.btoa('5') // FTP is 4th in this sort/direction
      },
      totalCount: 5
    }

    expect(actual).toEqual(expected)
  })

  it('returns correct data when last, sort, & direction are supplied', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData.slice(1).reverse()
        })
      )
    }

    const actual = await createConnection(
      {
        direction: 'DESC',
        last: 10,
        sort: 'marketCapUsd'
      },
      mockModel,
      assetsFieldMap
    )

    expect(mockModel.findAndCountAll).toHaveBeenCalledWith({
      limit: 10 + 1,
      offset: 0,
      order: [['market_cap', 'ASC NULLS LAST']],
      where: {}
    })
    const expected = {
      edges: normalizedData
        .map((asset, index) => ({
          cursor: Base64.btoa((index + 1).toString()),
          node: asset
        }))
        .slice(2),
      pageInfo: {
        endCursor: Base64.btoa('12'),
        hasNextPage: false,
        hasPreviousPage: true,
        startCursor: Base64.btoa('3')
      },
      totalCount: 10
    }
    expect(actual).toEqual(expected)
  })

  it('returns correct data when last, before, sort and direction are supplied', async () => {
    const mockModel = {
      count: jest.fn(() => Promise.resolve(12)),
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 12,
          rows: dbData
            .sort((a, b) => a.display_name.localeCompare(b.display_name))
            .slice(0, 3)
            .reverse()
        })
      )
    }

    const actual = await createConnection(
      {
        before: Base64.btoa('4'), // EOS is 4th with this sort/direction
        direction: 'ASC',
        last: 5,
        sort: 'name'
      },
      mockModel,
      assetsFieldMap
    )

    expect(mockModel.count).toHaveBeenCalledWith({ where: {} })
    expect(mockModel.findAndCountAll).toHaveBeenCalledWith({
      limit: 5 + 1,
      offset: dbData.length - 4 + 1,
      order: [['display_name', 'DESC NULLS LAST']],
      where: {}
    })
    // Expected [ 0x, Bitcoin, Bitcoin Cash ]
    const expected = {
      edges: normalizedData
        .sort((a, b) => a.name.localeCompare(b.name))
        .map((asset, index) => ({
          cursor: Base64.btoa((index + 1).toString()),
          node: asset
        }))
        .slice(0, 3),
      pageInfo: {
        endCursor: Base64.btoa('3'),
        hasNextPage: false,
        hasPreviousPage: false,
        startCursor: Base64.btoa('1')
      },
      totalCount: 3
    }
    expect(actual).toEqual(expected)
  })

  it('returns correct data when where, sort & direction are supplied', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 2,
          rows: [dbData[0], dbData[4]]
        })
      )
    }

    const actual = await createConnection(
      {
        direction: 'DESC',
        search: {
          columns: ['symbol', 'name'],
          searchTerm: 'bitcoi'
        },
        sort: 'marketCapUsd',
        where: { rank: 1 },
        first: 20
      },
      mockModel,
      assetsFieldMap
    )

    expect(mockModel.findAndCountAll).toHaveBeenCalledWith({
      limit: 20 + 1,
      offset: 0,
      order: [['market_cap', 'DESC NULLS LAST']],
      where: {
        rank: 1,
        $or: [
          {
            symbol: {
              $iLike: 'bitcoi%'
            }
          },
          {
            display_name: {
              $iLike: 'bitcoi%'
            }
          }
        ]
      }
    })
    const expected = {
      edges: [normalizedData[0], normalizedData[4]].map((asset, index) => ({
        cursor: Base64.btoa((index + 1).toString()),
        node: asset
      })),
      pageInfo: {
        endCursor: Base64.btoa('2'),
        hasNextPage: false,
        hasPreviousPage: false,
        startCursor: Base64.btoa('1')
      },
      totalCount: 2
    }
    expect(actual).toEqual(expected)
  })

  it('calls customQuery.db.query with correct params when customQuery argument is supplied', async () => {
    const mockModel = {
      count: jest.fn(() => Promise.resolve(12))
    }

    const customQuery = {
      db: {
        query: jest.fn(() => Promise.resolve([[]]))
      },
      queryString: `SELECT * from asset_markets_mv where WHERE base_slug = ?
            UNION
            SELECT * from asset_markets_mv WHERE quote_slug = ?) x`.replace(/\s/g, ''),
      replacements: ['bitcoin', 'bitcoin']
    }

    await createConnection(
      {
        direction: 'ASC',
        last: 5,
        sort: 'name'
      },
      mockModel,
      assetsFieldMap,
      customQuery
    )

    expect(customQuery.db.query).toHaveBeenCalledWith(
      `${`SELECT * from asset_markets_mv where WHERE base_slug = ?
            UNION
            SELECT * from asset_markets_mv WHERE quote_slug = ?) x`.replace(
        /\s/g,
        ''
      )} ORDER BY display_name DESC NULLS LAST LIMIT ? OFFSET ?;`,
      { replacements: ['bitcoin', 'bitcoin', 5 + 1, 0] }
    )
  })

  it('returns expected connection when customQuery argument is supplied', async () => {
    const mockModel = {
      count: jest.fn(() => Promise.resolve(12))
    }

    const customQuery = {
      db: {
        query: jest.fn(() => Promise.resolve(dbData.map(res => ({ ...res, full_count: 12 }))))
      },
      queryString: `SELECT * from asset_markets_mv where WHERE base_slug = ?
            UNION
            SELECT * from asset_markets_mv WHERE quote_slug = ?
            ORDER BY SORT DIRECTION) x
            LIMIT ?
            OFFSET ?;`,
      replacements: ['bitcoin', 'bitcoin']
    }

    const actual = await createConnection(
      {
        direction: 'DESC',
        sort: 'marketCapUsd',
        first: 20
      },
      mockModel,
      assetsFieldMap,
      customQuery
    )

    const expected = {
      edges: normalizedData.map((asset, index) => ({
        cursor: Base64.btoa((index + 1).toString()),
        node: asset
      })),
      pageInfo: {
        endCursor: Base64.btoa('12'),
        hasNextPage: false,
        hasPreviousPage: false,
        startCursor: Base64.btoa('1')
      },
      totalCount: 12
    }

    expect(actual).toEqual(expected)
  })

  it('handles the case when no results are returned from the database', async () => {
    const mockModel = {
      findAndCountAll: jest.fn(() =>
        Promise.resolve({
          count: 0,
          rows: []
        })
      )
    }

    const actual = await createConnection(
      {
        direction: 'DESC',
        where: {
          $or: [
            {
              display_name: {
                $iLike: 'XXXXXXXXXX%'
              }
            },
            {
              symbol: {
                $iLike: 'XXXXXXXXXX%'
              }
            }
          ]
        },
        sort: 'marketCapUsd',
        first: 2000
      },
      mockModel,
      assetsFieldMap
    )

    expect(mockModel.findAndCountAll).toHaveBeenCalledWith({
      limit: 2000 + 1,
      offset: 0,
      order: [['market_cap', 'DESC NULLS LAST']],
      where: {
        $or: [
          {
            display_name: {
              $iLike: 'XXXXXXXXXX%'
            }
          },
          {
            symbol: {
              $iLike: 'XXXXXXXXXX%'
            }
          }
        ]
      }
    })
    const expected = {
      edges: [],
      pageInfo: {
        endCursor: null,
        hasNextPage: false,
        hasPreviousPage: false,
        startCursor: null
      },
      totalCount: 0
    }
    expect(actual).toEqual(expected)
  })

  it('throws an error when after and before are passed in together', async () => {
    const args = {
      after: Base64.btoa('995'),
      before: Base64.btoa('995'),
      direction: 'ASC',
      first: 5,
      sort: 'name'
    }

    await expect(createConnection(args, {}, assetsFieldMap)).rejects.toThrowError(
      'Arguments after and before must not be together'
    )
  })

  it('throws an error when sort and direction are not supplied', async () => {
    const args = {
      after: Base64.btoa('995'),
      last: 5,
      sort: 'name'
    }

    await expect(createConnection(args, {}, assetsFieldMap)).rejects.toThrowError(
      'Arguments sort and direction are required'
    )
  })

  it('throws an error when fieldMap is not supplied correctly', async () => {
    const args = {
      after: Base64.btoa('995'),
      direction: 'ASC',
      last: 5,
      sort: 'name'
    }

    await expect(createConnection(args, {}, 'display_name')).rejects.toThrowError(
      'Argument fieldMap is required'
    )
  })
})

describe('mapDbFieldsToGraphQlFields', () => {
  it('correctly maps fields', () => {
    const dbResults = [
      {
        exchange_id: 'gatecoin',
        exchange_name: 'Gatecoin',
        rank: 22,
        trading_pairs: '6',
        base_id: 'd3d46cd5-6ec4-42c3-824c-0fff743d361e',
        base_symbol: 'ETH',
        base_slug: 'ethereum',
        quote_id: '353587d4-4376-4af7-99d1-cb2e212614d0',
        quote_symbol: 'BTC',
        quote_slug: 'bitcoin',
        usd_volume_24: '58014.4186754630313773',
        percent_usd_volume_24: '0.016370042249820922',
        updated: '2018-05-31T16:24:16.165Z',
        randomIgnoredField: 'blah blah blah'
      },
      {
        exchange_id: 'yobit',
        exchange_name: 'YoBit',
        rank: 15,
        trading_pairs: '6',
        base_id: '10a9c92a-130e-4686-a800-612dd75ebacf',
        base_symbol: 'DASH',
        base_slug: 'dash',
        quote_id: '353587d4-4376-4af7-99d1-cb2e212614d0',
        quote_symbol: 'BTC',
        quote_slug: 'bitcoin',
        usd_volume_24: '251343.263219825688093',
        percent_usd_volume_24: '0.070922021319100436',
        updated: '2018-05-31T16:24:16.165Z',
        randomIgnoredField: 'blah blah blah'
      }
    ]

    const expected = [
      {
        id: 'gatecoin',
        name: 'Gatecoin',
        rank: 22,
        tradingPairs: '6',
        topPairBaseSymbol: 'ETH',
        topPairBaseId: 'ethereum',
        topPairQuoteSymbol: 'BTC',
        topPairQuoteId: 'bitcoin',
        volumeUsd24Hr: '58014.4186754630313773',
        percentTotalVolume: '0.016370042249820922',
        updatedAt: '2018-05-31T16:24:16.165Z'
      },
      {
        id: 'yobit',
        name: 'YoBit',
        rank: 15,
        tradingPairs: '6',
        topPairBaseSymbol: 'DASH',
        topPairBaseId: 'dash',
        topPairQuoteSymbol: 'BTC',
        topPairQuoteId: 'bitcoin',
        volumeUsd24Hr: '251343.263219825688093',
        percentTotalVolume: '0.070922021319100436',
        updatedAt: '2018-05-31T16:24:16.165Z'
      }
    ]

    const actual = mapDbFieldsToGraphQlFields(dbResults, exchangesFieldMap)

    expect(actual).toEqual(expected)
  })

  describe('enrichArgs', () => {
    let args
    it('returns expected args when no defaults are needed and has a search', () => {
      args = {
        before: Base64.btoa('10'),
        direction: 'ASC',
        last: 5,
        sort: 'name',
        search: 'bitcoin'
      }

      const expectedArgs = {
        before: Base64.btoa('10'),
        direction: 'ASC',
        last: 5,
        sort: 'name',
        search: {
          columns: ['symbol, name'],
          searchTerm: 'bitcoin'
        },
        where: {}
      }
      const enrichedArgs = enrichArgs({
        args,
        searchColumns: ['symbol, name'],
        defaultDirection: 'DESC',
        defaultSort: 'marketCapUsd'
      })

      expect(enrichedArgs).toEqual(expectedArgs)
    })

    it('returns expected args when defaults are needed and has a search', () => {
      args = {
        before: Base64.btoa('10'),
        last: 5,
        search: 'bitcoin'
      }

      const expectedArgs = {
        before: Base64.btoa('10'),
        direction: 'DESC',
        last: 5,
        sort: 'marketCapUsd',
        search: {
          columns: ['symbol, name'],
          searchTerm: 'bitcoin'
        },
        where: {}
      }
      const enrichedArgs = enrichArgs({
        args,
        searchColumns: ['symbol, name'],
        defaultDirection: 'DESC',
        defaultSort: 'marketCapUsd'
      })

      expect(enrichedArgs).toEqual(expectedArgs)
    })

    it('returns expected args when defaults are needed and no search', () => {
      args = {
        before: Base64.btoa('10'),
        last: 5
      }

      const expectedArgs = {
        before: Base64.btoa('10'),
        direction: 'DESC',
        last: 5,
        sort: 'marketCapUsd',
        search: undefined,
        where: {}
      }
      const enrichedArgs = enrichArgs({
        args,
        searchColumns: ['symbol, name'],
        defaultDirection: 'DESC',
        defaultSort: 'marketCapUsd'
      })

      expect(enrichedArgs).toEqual(expectedArgs)
    })
  })
})

describe('handleError', () => {
  let error
  let wrapperFunc

  it('rethrows validation errors', () => {
    error = new Error('Validation error: Arguments after and before must not be together')
    wrapperFunc = () => handleError(error)

    expect(wrapperFunc).toThrowError(error)
  })

  it('throws a generic error for all other errors', () => {
    error = new Error('SQL Error: assets_mv does not exist')
    wrapperFunc = () => handleError(error)

    expect(wrapperFunc).toThrowError(
      'An unexpected error has occurred. Please try back again later.'
    )
  })
})

describe('deleteAndReturn', () => {
  let destroy
  let findOne
  let model
  let where

  beforeEach(() => {
    destroy = jest.fn(() => 1)
    findOne = jest.fn(() => ({
      id: 'this is a guid',
      name: 'lambo',
      user_id: 'this is a user id',
      created: '2019-01-25T22:43:29.993Z',
      updated: '2019-01-25T22:43:29.993Z',
      deleted: ''
    }))
    model = {
      destroy,
      findOne
    }
    where = {
      asset_id: 'bitcoin',
      portfolio_id: 'this-is-a-uuid'
    }
  })

  it('calls model.findOne', async () => {
    await deleteAndReturn(model, where)
    expect(findOne).toHaveBeenCalledWith({ where })
  })

  it('calls model.destroy', async () => {
    await deleteAndReturn(model, where)
    expect(destroy).toHaveBeenCalledWith({ where })
  })

  it('returns the deleted object', async () => {
    const actual = await deleteAndReturn(model, where)
    expect(actual).toEqual({
      id: 'this is a guid',
      name: 'lambo',
      user_id: 'this is a user id',
      created: '2019-01-25T22:43:29.993Z',
      updated: '2019-01-25T22:43:29.993Z',
      deleted: ''
    })
  })

  it('throws an exception when destroy fails', async () => {
    model.destroy = jest.fn(() => 0)
    await expect(deleteAndReturn(model, where)).rejects.toThrowError(
      'Validation Error: Delete failed!'
    )
  })
})
