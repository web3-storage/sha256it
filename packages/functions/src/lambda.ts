import crypto from 'node:crypto'
import { ApiHandler } from "sst/node/api"
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import * as Link from 'multiformats/link'
import * as Digest from 'multiformats/hashes/digest'
import { sha256 } from 'multiformats/hashes/sha2'

const CAR_CODEC = 0x0202

export const handler = ApiHandler(async event => {
  const { searchParams } = new URL(`http://localhost/?${event.rawQueryString}`)

  const region = searchParams.get('region')
  if (!region) return { statusCode: 400, body: 'Missing "region" search parameter' }
  if (!['us-east-1', 'us-west-2'].includes(region)) return { statusCode: 400, body: 'Invalid region' }

  const bucket = searchParams.get('bucket')
  if (!bucket) return { statusCode: 400, body: 'Missing "bucket" search parameter' }
  if (!bucket.startsWith('dotstorage')) return { statusCode: 400, body: 'Invalid bucket' }

  const key = searchParams.get('key')
  if (!key) return { statusCode: 400, body: 'Missing "key" search parameter' }
  if (!key.endsWith('.car')) return { statusCode: 400, body: 'Only keys for CARs supported' }

  const s3 = new S3Client({ region })
  const cmd = new GetObjectCommand({ Bucket: bucket, Key: key })

  const res = await s3.send(cmd)
  if (!res.Body) return { statusCode: 404, body: 'Object not found' }

  const hash = crypto.createHash('sha256')
  await res.Body.transformToWebStream()
    .pipeTo(new WritableStream({ write: chunk => { hash.update(chunk) } }))

  const digest = Digest.create(sha256.code, hash.digest())
  const cid = Link.create(CAR_CODEC, digest)

  return { statusCode: 200, body: `{"/":"${cid}"}` }
})
