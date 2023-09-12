import { expect, test, beforeAll, beforeEach, afterAll, afterEach, Nullable } from 'vitest'
import fs from 'node:fs'
import { Readable } from 'node:stream'
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { DynamoDBClient, GetItemCommand, PutItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall } from '@aws-sdk/util-dynamodb'
import { UnknownLink, Link } from 'multiformats'
import { base58btc } from 'multiformats/bases/base58'
import { CARReaderStream } from 'carstream'
import { Block, Position } from 'carstream/api'
import varint from 'varint'
import { TestAWSService, createDynamo, createDynamoTable, createS3, createS3Bucket, itemExists, keyExists } from './helpers/aws'
import { generateTestCAR, writeCARIndex } from './helpers/car'
import { index } from '../src/index'

let s3: TestAWSService<S3Client>
let dynamo: TestAWSService<DynamoDBClient>
let srcBucket: string
let blockIndexTable: string
let srcCAR: Nullable<{ cid: Link<Uint8Array, 0x0202>, root: UnknownLink, size: number, path: string }>
let srcIndex: Nullable<{ size: number, path: string, items: Array<{ cid: UnknownLink } & Position> }>

beforeAll(async () => {
  s3 = await createS3()
  dynamo = await createDynamo()
})

beforeEach(async () => {
  srcBucket = await createS3Bucket(s3.client, 'src')
  blockIndexTable = await createDynamoTable(dynamo.client, 'blocks-cars-position')
  srcCAR = null
  srcIndex = null
})

afterEach(async () => {
  if (srcCAR) await fs.promises.rm(srcCAR.path)
  if (srcIndex) await fs.promises.rm(srcIndex.path)
})

afterAll(async () => {
  await s3.container.stop()
  await dynamo.container.stop()
})

const toBlockIndexItem = (block: Block & Position, region: string, bucket: string, key: string) => {
  const blockHeaderLength = varint.encodingLength(block.length) + block.cid.bytes.length
  return {
    carpath: `${region}/${bucket}/${key}`,
    blockmultihash: base58btc.encode(block.cid.multihash.bytes),
    offset: block.offset + blockHeaderLength,
    length: block.length - blockHeaderLength
  }
}

test('index a CAR with CARv2 side index', async () => {
  srcCAR = await generateTestCAR(50 * 1024 * 1024)
  srcIndex = await writeCARIndex(srcCAR.path, `${srcCAR.path}.idx`)

  const srcCARKey = `complete/${srcCAR.root}.car`
  await s3.client.send(new PutObjectCommand({
    Bucket: srcBucket,
    Key: srcCARKey,
    ContentLength: srcCAR.size,
    Body: fs.createReadStream(srcCAR.path)
  }))

  const srcIndexKey = `complete/${srcCAR.root}.car.idx`
  await s3.client.send(new PutObjectCommand({
    Bucket: srcBucket,
    Key: srcIndexKey,
    ContentLength: srcIndex.size,
    Body: fs.createReadStream(srcIndex.path)
  }))

  const stream = Readable.toWeb(fs.createReadStream(srcCAR.path)) as ReadableStream<Uint8Array>
  await stream
    .pipeThrough(new CARReaderStream())
    .pipeTo(new WritableStream({
      async write (block) {
        await dynamo.client.send(new PutItemCommand({
          TableName: blockIndexTable,
          Item: marshall(toBlockIndexItem(block, s3.region, srcBucket, srcCARKey))
        }))
      }
    }))

  const res = await index({
    ...s3,
    cid: srcCAR.cid,
    bucket: srcBucket,
    key: srcCARKey
  }, {
    ...dynamo,
    tableName: blockIndexTable
  })
  expect(res.statusCode).toBe(200)

  const { ok, updated } = JSON.parse(res.body)
  expect(ok).toBe(true)
  expect(updated).toBe(srcIndex.items.length)

  for (const item of srcIndex.items) {
    const blockmultihash = base58btc.encode(item.cid.multihash.bytes)
    await expect(itemExists(dynamo.client, blockIndexTable, {
      blockmultihash,
      carpath: `auto/carpark-prod-0/${srcCAR.cid}/${srcCAR.cid}.car`
    })).resolves.toBe(true)
    await expect(itemExists(dynamo.client, blockIndexTable, {
      blockmultihash,
      carpath: `${s3.region}/${srcBucket}/${srcCARKey}`
    })).resolves.toBe(false)
  }
})

test('index a CAR without CARv2 side index', async () => {
  srcCAR = await generateTestCAR(50 * 1024 * 1024)

  const srcCARKey = `complete/${srcCAR.root}.car`
  await s3.client.send(new PutObjectCommand({
    Bucket: srcBucket,
    Key: srcCARKey,
    ContentLength: srcCAR.size,
    Body: fs.createReadStream(srcCAR.path)
  }))

  const blocks: Block[] = []
  const stream = Readable.toWeb(fs.createReadStream(srcCAR.path)) as ReadableStream<Uint8Array>
  await stream
    .pipeThrough(new CARReaderStream())
    .pipeTo(new WritableStream({
      async write (block) {
        blocks.push(block)
        await dynamo.client.send(new PutItemCommand({
          TableName: blockIndexTable,
          Item: marshall(toBlockIndexItem(block, s3.region, srcBucket, srcCARKey))
        }))
      }
    }))

  const res = await index({
    ...s3,
    cid: srcCAR.cid,
    bucket: srcBucket,
    key: srcCARKey
  }, {
    ...dynamo,
    tableName: blockIndexTable
  })
  expect(res.statusCode).toBe(200)

  const { ok, updated } = JSON.parse(res.body)
  expect(ok).toBe(true)
  expect(updated).toBe(blocks.length)

  for (const block of blocks) {
    const blockmultihash = base58btc.encode(block.cid.multihash.bytes)
    await expect(itemExists(dynamo.client, blockIndexTable, {
      blockmultihash,
      carpath: `auto/carpark-prod-0/${srcCAR.cid}/${srcCAR.cid}.car`
    })).resolves.toBe(true)
    await expect(itemExists(dynamo.client, blockIndexTable, {
      blockmultihash,
      carpath: `${s3.region}/${srcBucket}/${srcCARKey}`
    })).resolves.toBe(false)
  }
})

test('do not touch other block index items for same multihash', async () => {
  srcCAR = await generateTestCAR(5 * 1024 * 1024)

  const srcCARKey = `complete/${srcCAR.root}.car`
  await s3.client.send(new PutObjectCommand({
    Bucket: srcBucket,
    Key: srcCARKey,
    ContentLength: srcCAR.size,
    Body: fs.createReadStream(srcCAR.path)
  }))
  const altCARKey = `raw/u/${srcCAR.root}/${srcCAR.cid}.car`

  const blocks: Block[] = []
  const stream = Readable.toWeb(fs.createReadStream(srcCAR.path)) as ReadableStream<Uint8Array>
  await stream
    .pipeThrough(new CARReaderStream())
    .pipeTo(new WritableStream({
      async write (block) {
        blocks.push(block)
        await dynamo.client.send(new PutItemCommand({
          TableName: blockIndexTable,
          Item: marshall(toBlockIndexItem(block, s3.region, srcBucket, srcCARKey))
        }))
        await dynamo.client.send(new PutItemCommand({
          TableName: blockIndexTable,
          Item: marshall(toBlockIndexItem(block, s3.region, srcBucket, altCARKey))
        }))
      }
    }))

  const res = await index({
    ...s3,
    cid: srcCAR.cid,
    bucket: srcBucket,
    key: srcCARKey
  }, {
    ...dynamo,
    tableName: blockIndexTable
  })
  expect(res.statusCode).toBe(200)

  const { ok, updated } = JSON.parse(res.body)
  expect(ok).toBe(true)
  expect(updated).toBe(blocks.length)

  for (const block of blocks) {
    const blockmultihash = base58btc.encode(block.cid.multihash.bytes)
    await expect(itemExists(dynamo.client, blockIndexTable, {
      blockmultihash,
      carpath: `auto/carpark-prod-0/${srcCAR.cid}/${srcCAR.cid}.car`
    })).resolves.toBe(true)
    await expect(itemExists(dynamo.client, blockIndexTable, {
      blockmultihash,
      carpath: `${s3.region}/${srcBucket}/${srcCARKey}`
    })).resolves.toBe(false)
    await expect(itemExists(dynamo.client, blockIndexTable, {
      blockmultihash,
      carpath: `${s3.region}/${srcBucket}/${altCARKey}`
    })).resolves.toBe(true)
  }
})
