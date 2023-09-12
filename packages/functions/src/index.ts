import { ApiHandler } from 'sst/node/api'
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { MultihashDigest } from 'multiformats'
import * as Link from 'multiformats/link'
import { base58btc } from 'multiformats/bases/base58'
import { CARReaderStream } from 'carstream'
import { mustGetEnv, errorResponse } from './lib/util'
import { BatchGetItemCommand, BatchWriteItemCommand, DynamoDBClient, WriteRequest } from '@aws-sdk/client-dynamodb'
import { Block, Position } from 'carstream/api'
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb'
import retry from 'p-retry'
import { Parallel } from 'parallel-transform-web'
import { MultihashIndexSortedReader } from 'cardex/multihash-index-sorted'
import { Credentials, Endpoint, ShardLink, ShardObjectID } from './lib/api.js'
import { CAR_CODEC } from './lib/constants.js'

interface TableID extends Endpoint, Credentials {
  tableName: string
}

export const handler = ApiHandler(event => _handler.call(null, new Request(`http://localhost/?${event.rawQueryString}`), process.env))

export const _handler = async (request: Request, env: Record<string, string|undefined>) => {
  try {
    const { searchParams } = new URL(request.url)

    const srcRegion = searchParams.get('region')
    if (!srcRegion) return errorResponse('Missing "region" search parameter', 400)
    if (!['us-east-2', 'us-west-2'].includes(srcRegion)) return errorResponse('Invalid region', 400)

    const srcBucketName = searchParams.get('bucket')
    if (!srcBucketName) return errorResponse('Missing "bucket" search parameter', 400)
    if (!srcBucketName.startsWith('dotstorage')) return errorResponse('Invalid bucket', 400)

    const srcKey = searchParams.get('key')
    if (!srcKey) return errorResponse('Missing "key" search parameter', 400)
    if (!srcKey.endsWith('.car')) return errorResponse('Only keys for CARs supported', 400)

    const shardstr = searchParams.get('shard')
    if (!shardstr) return errorResponse('Missing "shard" search parameter', 400)
    const shard: ShardLink = Link.parse(shardstr)
    if (shard.code !== CAR_CODEC) return errorResponse('Not a CAR file hash', 400)

    const src = {
      cid: shard,
      region: srcRegion,
      bucket: srcBucketName,
      key: srcKey
    }

    const dest = {
      region: mustGetEnv(env, 'BLOCK_INDEX_REGION'),
      tableName: mustGetEnv(env, 'BLOCK_INDEX_TABLE')
    }

    return await index(src, dest)
  } catch (err: any) {
    console.error(err)
    return errorResponse(err.message, 500)
  }
}

class Batcher<I> extends TransformStream<I, I[]> {
  constructor (size: number) {
    let batch: I[] = []
    super({
      transform (chunk, controller) {
        batch.push(chunk)
        if (batch.length < size) return
        controller.enqueue(batch)
        batch = []
      },
      flush (controller) {
        if (batch.length) controller.enqueue(batch)
        batch = []
      }
    })
  }
}

interface BlockIndexItem {
  blockmultihash: string
  carpath: string
  offset: number
  length: number
}

export const index = async (src: ShardObjectID, dest: TableID) => {
  const dynamo = new DynamoDBClient(dest)
  const multihashes = await shardMultihashes(src)
  let total = 0
  await multihashes
    .pipeThrough(new Batcher(100))
    .pipeThrough(new TransformStream<MultihashDigest[], BlockIndexItem>({
      async transform (batch, controller) {
        const cmd = new BatchGetItemCommand({
          RequestItems: {
            [dest.tableName]: {
              Keys: batch.map(multihash => marshall({
                blockmultihash: base58btc.encode(multihash.bytes),
                carpath: `${src.region}/${src.bucket}/${src.key}`
              }))
            }
          }
        })
        const res = await dynamo.send(cmd)
        for (const item of res.Responses?.[dest.tableName] ?? []) {
          controller.enqueue(unmarshall(item) as BlockIndexItem)
        }
      }
    }))
    .pipeThrough(new Batcher(25))
    .pipeThrough(new Parallel(5, async batch => {
      const writeItems = (items: WriteRequest[]) => retry(async () => {
        const cmd = new BatchWriteItemCommand({ RequestItems: { [dest.tableName]: items } })
        const res = await dynamo.send(cmd)
        if (res.UnprocessedItems && res.UnprocessedItems[dest.tableName]?.length) {
          items = res.UnprocessedItems[dest.tableName]
          throw new Error(`${res.UnprocessedItems[dest.tableName].length} unprocessed items`)
        }
      }, { retries: 2 })

      let items: WriteRequest[] = batch.map(b => {
        const item = { ...b, carpath: `auto/carpark-prod-0/${src.cid}/${src.cid}.car` }
        console.warn('write', JSON.stringify(item))
        return { PutRequest: { Item: marshall(item) } }
      })

      // write new items
      await writeItems(items)

      items = batch.map(b => {
        const key = { blockmultihash: b.blockmultihash, carpath: b.carpath }
        console.warn('delete', JSON.stringify(key))
        return { DeleteRequest: { Key: marshall(key) } }
      })

      // delete old items
      await writeItems(items)

      return batch.length
    }))
    .pipeTo(new WritableStream({
      async write (updated) {
        total += updated
      }
    }))

  return { statusCode: 200, body: JSON.stringify({ ok: true, updated: total }) }
}

/** Retrieve multihashes of blocks in the CAR. */
const shardMultihashes = async (src: ShardObjectID): Promise<ReadableStream<MultihashDigest>> => {
  const s3 = new S3Client(src)
  try {
    const cmd = new GetObjectCommand({ Bucket: src.bucket, Key: `${src.key}.idx` })
    const res = await s3.send(cmd)
    if (!res.Body) throw new Error('missing body')
    const reader = MultihashIndexSortedReader.createReader({ reader: res.Body.transformToWebStream().getReader() })
    return new ReadableStream({
      async pull (controller) {
        const { done, value } = await reader.read()
        if (done) return controller.close()
        controller.enqueue(value.multihash)
      }
    })
  } catch (err: any) {
    if (err.$metadata?.httpStatusCode !== 404) {
      console.error(`failed to read index: ${src.key}.idx`, err)
      throw new Error(`failed to read index: ${src.key}.idx`, { cause: err })
    }
  }

  const blocks = await shardBlocks(src)
  return blocks.pipeThrough(new TransformStream({
    transform (block, controller) {
      controller.enqueue(block.cid.multihash)
    }
  }))
}

/** Retrieve blocks from the CAR. */
const shardBlocks = async (src: ShardObjectID): Promise<ReadableStream<Block & Position>> => {
  const s3 = new S3Client(src)
  const cmd = new GetObjectCommand({ Bucket: src.bucket, Key: src.key })
  const res = await s3.send(cmd)
  if (!res.Body) throw new Error('missing body')
  return res.Body.transformToWebStream().pipeThrough(new CARReaderStream())
}
