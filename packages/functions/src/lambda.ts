import crypto from 'node:crypto'
import { ApiHandler } from "sst/node/api"
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3'
import * as Link from 'multiformats/link'
import * as Digest from 'multiformats/hashes/digest'
import { sha256 } from 'multiformats/hashes/sha2'

const CAR_CODEC = 0x0202

export const handler = ApiHandler(async event => {
  const key = new URL(`http://localhost/?${event.rawQueryString}`).searchParams.get('key')
  if (!key) return { statusCode: 400, body: 'Missing "key" search parameter' }
  if (!key.endsWith('.car')) return { statusCode: 400, body: 'Only keys for CARs supported' }

  const s3 = new S3Client({ region: notNully('REGION', process.env) })
  const bucket = notNully('BUCKET', process.env)
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

const notNully = (k: string, obj: Record<string, string|undefined>) => {
  const v = obj[k]
  if (!v) throw new Error(`${k} must not be null`)
  return v
}
