import crypto from 'node:crypto'
import { ApiHandler } from 'sst/node/api'
import { AbortMultipartUploadCommand, CompleteMultipartUploadCommand, CompletedPart, CreateMultipartUploadCommand, GetObjectCommand, HeadObjectCommand, PutObjectCommand, S3Client, UploadPartCommand } from '@aws-sdk/client-s3'
import { Readable } from 'node:stream'
import * as Link from 'multiformats/link'
import { UnknownLink } from 'multiformats/link'
import * as Digest from 'multiformats/hashes/digest'
import { sha256 } from 'multiformats/hashes/sha2'
import { base64pad } from 'multiformats/bases/base64'
import { Uint8ArrayList } from 'uint8arraylist'
import { CARReaderStream } from 'carstream'
import { MultihashIndexSortedWriter } from 'cardex/multihash-index-sorted'
import { mustGetEnv, errorResponse } from './lib/util'

const CAR_CODEC = 0x0202
const MAX_PUT_SIZE = 1024 * 1024 * 1024 * 5
const TARGET_PART_SIZE = 1024 * 1024 * 100

type ShardLink = Link.Link<Uint8Array, typeof CAR_CODEC>

interface ObjectID {
  region: string
  bucket: string
  key: string
  endpoint?: string
  credentials?: {
    accessKeyId: string,
    secretAccessKey: string
  }
}

interface ContentAddressedObjectID<
  Data extends unknown = unknown,
  Format extends number = number,
  Alg extends number = number,
  V extends Link.Version = 1
> extends ObjectID {
  cid: Link.Link<Data, Format, Alg, V>
}

interface ShardObjectID extends ContentAddressedObjectID<Uint8Array, typeof CAR_CODEC> {}

interface ShardSource extends ContentAddressedObjectID<Uint8Array, typeof CAR_CODEC> {
  size: number
  body: ReadableStream<Uint8Array>
}

interface PartSource {
  uploadID: string
  partNumber: number
  body: Uint8Array
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

    const rootstr = searchParams.get('root')
    if (!rootstr) return errorResponse('Missing "root" search parameter', 400)
    const root: UnknownLink = Link.parse(rootstr).toV1()

    const src = {
      cid: shard,
      region: srcRegion,
      bucket: srcBucketName,
      key: srcKey
    }

    const dest = {
      endpoint: mustGetEnv(env, 'DEST_ENDPOINT'),
      region: mustGetEnv(env, 'DEST_REGION'),
      credentials: {
        accessKeyId: mustGetEnv(env, 'DEST_ACCESS_KEY_ID'),
        secretAccessKey: mustGetEnv(env, 'DEST_SECRET_ACCESS_KEY')
      }
    }

    return await copy(src, {
      bucket: mustGetEnv(env, 'CARPARK_BUCKET'),
      key: `${shard}/${shard}.car`,
      ...dest
    }, {
      bucket: mustGetEnv(env, 'SATNAV_BUCKET'),
      key: `${shard}/${shard}.car.idx`,
      ...dest
    }, {
      bucket: mustGetEnv(env, 'DUDEWHERE_BUCKET'),
      key: `${root}/${shard}`,
      ...dest
    })
  } catch (err: any) {
    console.error(err)
    return errorResponse(err.message, 500)
  }
}

export const copy = async (src: ShardObjectID, dest: ObjectID, indexDest: ObjectID, linkDest: ObjectID, options?: { maxPutSize?: number }) => {
  try {
    console.log(`HeadObject ${dest.region}/${dest.bucket}/${dest.key}`)
    await s3Client(dest).send(new HeadObjectCommand({ Bucket: dest.bucket, Key: dest.key }))
    return { statusCode: 200, body: JSON.stringify({ ok: true }) } // already exists 🙌
  } catch (err: any) {
    if (err.$metadata?.httpStatusCode !== 404) {
      console.error(err)
      return errorResponse('Failed to determine if object exists at destination', 500)
    }
  }

  console.log(`GetObject ${src.region}/${src.bucket}/${src.key}`)
  const getCmd = new GetObjectCommand({ Bucket: src.bucket, Key: src.key })
  const getRes = await s3Client(src).send(getCmd)
  if (!getRes.Body) return errorResponse('Object not found', 404)
  if (!getRes.ContentLength) return errorResponse('Object has no size', 404)

  const [srcReadable0, srcReadable1] = getRes.Body.transformToWebStream().tee()
  await Promise.all([
    writeCAR({
      ...src,
      size: getRes.ContentLength,
      body: srcReadable0
    }, dest, options),
    writeCARIndex({
      ...src,
      size: getRes.ContentLength,
      body: srcReadable1
    }, indexDest),
    s3Client(linkDest).send(new PutObjectCommand({
      Bucket: linkDest.bucket,
      Key: linkDest.key,
      Body: new Uint8Array()
    }))
  ])

  return { statusCode: 200, body: JSON.stringify({ ok: true }) }
}

const writeCAR = async (src: ShardSource, dest: ObjectID, options?: { maxPutSize?: number }) => {
  const maxPutSize = options?.maxPutSize ?? MAX_PUT_SIZE
  // for small files, just do a regular put with ChecksumSHA256
  if (src.size < maxPutSize) {
    console.log(`PutObject ${src.region}/${src.bucket}/${src.key} => ${dest.region}/${dest.bucket}/${dest.key}`)
    return s3Client(dest).send(new PutObjectCommand({
      Bucket: dest.bucket,
      Key: dest.key,
      // @ts-expect-error
      Body: Readable.fromWeb(src.body),
      ContentLength: src.size,
      ChecksumSHA256: base64pad.encode(src.cid.multihash.digest).slice(1)
    }))
  }

  const hasher = crypto.createHash('sha256')
  const buffer = new Uint8ArrayList()
  const parts: CompletedPart[] = []
  let uploadID: string

  await src.body.pipeTo(new WritableStream({
    async start () {
      console.log(`CreateMultipartUpload ${src.region}/${src.bucket}/${src.key} => ${dest.region}/${dest.bucket}/${dest.key}`)
      const res = await s3Client(dest).send(new CreateMultipartUploadCommand({ Bucket: dest.bucket, Key: dest.key }))
      if (!res.UploadId) throw new Error('missing multipart upload ID')
      uploadID = res.UploadId
    },
    async write (chunk) {
      buffer.append(chunk)
      hasher.update(chunk)
      if (buffer.length >= TARGET_PART_SIZE) {
        const part = await uploadPart({ uploadID, partNumber: parts.length, body: buffer.subarray() }, dest)
        parts.push(part)
        buffer.consume(buffer.length)
      }
    },
    async close () {
      if (buffer.length) {
        const part = await uploadPart({ uploadID, partNumber: parts.length, body: buffer.subarray() }, dest)
        parts.push(part)
      }

      const digest = Digest.create(sha256.code, hasher.digest())
      if (Link.create(CAR_CODEC, digest).toString() !== src.cid.toString()) {
        console.log(`AbortMultipartUpload ${src.region}/${src.bucket}/${src.key} => ${dest.region}/${dest.bucket}/${dest.key}`)
        await s3Client(dest).send(new AbortMultipartUploadCommand({ Bucket: dest.bucket, Key: dest.key, UploadId: uploadID }))
        throw new Error('integrity check failed')
      }

      console.log(`CompleteMultipartUpload ${src.region}/${src.bucket}/${src.key} => ${dest.region}/${dest.bucket}/${dest.key}`)
      await s3Client(dest).send(new CompleteMultipartUploadCommand({
        Bucket: dest.bucket,
        Key: dest.key,
        UploadId: uploadID,
        MultipartUpload: { Parts: parts }
      }))
    }
  }))
}

const uploadPart = async (src: PartSource, dest: ObjectID): Promise<CompletedPart> => {
  console.log(`UploadPart ${src.uploadID} (#${src.partNumber}) => ${dest.region}/${dest.bucket}/${dest.key}`)
  const digest = await sha256.digest(src.body)
  const checksum = base64pad.encode(digest.digest).slice(1)
  const res = await s3Client(dest).send(new UploadPartCommand({
    UploadId: src.uploadID,
    PartNumber: src.partNumber,
    Bucket: dest.bucket,
    Key: dest.key,
    Body: src.body,
    ContentLength: src.body.length,
    ChecksumSHA256: checksum
  }))
  return { ETag: res.ETag, PartNumber: src.partNumber, ChecksumSHA256: checksum }
}

const writeCARIndex = async (src: ShardSource, dest: ObjectID) => {
  const { readable, writable } = new TransformStream()
  const writer = MultihashIndexSortedWriter.createWriter({ writer: writable.getWriter() })
  const chunks: Uint8Array[] = []
  await Promise.all([
    src.body
      .pipeThrough(new CARReaderStream())
      .pipeTo(new WritableStream({
        async write (block) {
          await writer.add(block.cid, block.offset)
        },
        async close () {
          await writer.close()
        }
      })),
    readable.pipeTo(new WritableStream({ write: chunk => { chunks.push(chunk) } }))
  ])
  await s3Client(dest).send(new PutObjectCommand({
    Bucket: dest.bucket,
    Key: dest.key,
    Body: Uint8ArrayList.fromUint8Arrays(chunks).subarray()
  }))
}

const s3Client = ({ region, endpoint, credentials }: {
  region: string
  endpoint?: string
  credentials?: {
    accessKeyId: string,
    secretAccessKey: string
  }
}) => new S3Client({ region, endpoint, credentials })
