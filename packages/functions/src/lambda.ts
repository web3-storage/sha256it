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
  if (!region) return errorResponse('Missing "region" search parameter', 400)
  if (!['us-east-2', 'us-west-2'].includes(region)) return errorResponse('Invalid region', 400)

  const bucket = searchParams.get('bucket')
  if (!bucket) return errorResponse('Missing "bucket" search parameter', 400)
  if (!bucket.startsWith('dotstorage')) return errorResponse('Invalid bucket', 400)

  const key = searchParams.get('key')
  if (!key) return errorResponse('Missing "key" search parameter', 400)
  if (!key.endsWith('.car')) return errorResponse('Only keys for CARs supported', 400)

  const s3 = new S3Client({ region })
  const cmd = new GetObjectCommand({ Bucket: bucket, Key: key })

  const res = await s3.send(cmd)
  if (!res.Body) return errorResponse('Object not found', 404)

  const hash = crypto.createHash('sha256')
  await res.Body.transformToWebStream()
    .pipeTo(new WritableStream({ write: chunk => { hash.update(chunk) } }))

  const digest = Digest.create(sha256.code, hash.digest())
  const cid = Link.create(CAR_CODEC, digest)

  return { statusCode: 200, body: `{"ok":true,"cid":{"/":"${cid}"}}` }
})

const errorResponse = (message: string, statusCode = 500) => ({
  statusCode,
  body: JSON.stringify({ ok: false, error: message })
})
