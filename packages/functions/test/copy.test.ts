import { expect, test, beforeAll, beforeEach, afterAll, afterEach, Nullable } from 'vitest'
import fs from 'node:fs'
import { PutObjectCommand, S3Client } from '@aws-sdk/client-s3'
import { UnknownLink, Link } from 'multiformats'
import { TestAWSService, createS3, createS3Bucket, keyExists } from './helpers/aws'
import { generateTestCAR } from './helpers/car'
import { copy } from '../src/copy'

let s3: TestAWSService<S3Client>
let srcBucket: string
let carparkBucket: string
let satnavBucket: string
let dudewhereBucket: string
let srcCAR: Nullable<{ cid: Link<Uint8Array, 0x0202>, root: UnknownLink, size: number, path: string }>

beforeAll(async () => {
  s3 = await createS3()
})

beforeEach(async () => {
  srcBucket = await createS3Bucket(s3.client, 'src')
  carparkBucket = await createS3Bucket(s3.client, 'carpark')
  satnavBucket = await createS3Bucket(s3.client, 'satnav')
  dudewhereBucket = await createS3Bucket(s3.client, 'dudewhere')
  srcCAR = null
})

afterEach(async () => {
  if (srcCAR) await fs.promises.rm(srcCAR.path)
})

afterAll(async () => {
  await s3.container.stop()
})

test('copy a small CAR', async () => {
  srcCAR = await generateTestCAR(5 * 1024 * 1024)
  const srcKey = `complete/${srcCAR.root}.car`

  await s3.client.send(new PutObjectCommand({
    Bucket: srcBucket,
    Key: srcKey,
    ContentLength: srcCAR.size,
    Body: fs.createReadStream(srcCAR.path)
  }))

  const carparkKey = `${srcCAR.cid}/${srcCAR.cid}.car`
  const satnavKey = `${srcCAR.cid}/${srcCAR.cid}.car.idx`
  const dudewhereKey = `${srcCAR.root}/${srcCAR.cid}`

  const res = await copy({
    ...s3,
    cid: srcCAR.cid,
    bucket: srcBucket,
    key: srcKey
  }, {
    ...s3,
    bucket: carparkBucket,
    key: carparkKey
  }, {
    ...s3,
    bucket: satnavBucket,
    key: satnavKey
  }, {
    ...s3,
    bucket: dudewhereBucket,
    key: dudewhereKey
  })
  expect(res.statusCode).toBe(200)

  await expect(keyExists(s3.client, carparkBucket, carparkKey)).resolves.toBe(true)
  await expect(keyExists(s3.client, satnavBucket, satnavKey)).resolves.toBe(true)
  await expect(keyExists(s3.client, dudewhereBucket, dudewhereKey)).resolves.toBe(true)
})

test('copy a large CAR with multipart', async () => {
  srcCAR = await generateTestCAR(500 * 1024 * 1024)
  const srcKey = `complete/${srcCAR.root}.car`

  await s3.client.send(new PutObjectCommand({
    Bucket: srcBucket,
    Key: srcKey,
    ContentLength: srcCAR.size,
    Body: fs.createReadStream(srcCAR.path)
  }))

  const carparkKey = `${srcCAR.cid}/${srcCAR.cid}.car`
  const satnavKey = `${srcCAR.cid}/${srcCAR.cid}.car.idx`
  const dudewhereKey = `${srcCAR.root}/${srcCAR.cid}`

  console.time('copy')
  const res = await copy({
    ...s3,
    cid: srcCAR.cid,
    bucket: srcBucket,
    key: srcKey
  }, {
    ...s3,
    bucket: carparkBucket,
    key: carparkKey
  }, {
    ...s3,
    bucket: satnavBucket,
    key: satnavKey
  }, {
    ...s3,
    bucket: dudewhereBucket,
    key: dudewhereKey
  }, { maxPutSize: 1024 * 1024 * 50 })
  expect(res.statusCode).toBe(200)
  console.timeEnd('copy')

  await expect(keyExists(s3.client, carparkBucket, carparkKey)).resolves.toBe(true)
  await expect(keyExists(s3.client, satnavBucket, satnavKey)).resolves.toBe(true)
  await expect(keyExists(s3.client, dudewhereBucket, dudewhereKey)).resolves.toBe(true)
}, { timeout: 60_000 })
