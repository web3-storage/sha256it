#!/usr/bin/env node
import fs from 'node:fs'
import { Readable, Writable } from 'node:stream'
import sade from 'sade'
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3'
import dotenv from 'dotenv'
import { Parse } from 'ndjson-web'
import * as dagJSON from '@ipld/dag-json'
import { Parallel } from 'parallel-transform-web'
import retry from 'p-retry'
import * as Link from 'multiformats/link'

const pkg = JSON.parse(fs.readFileSync(new URL('./package.json', import.meta.url)).toString())
const cli = sade('sha256it')
const concurrency = 25

dotenv.config({ path: './.env.local' })

cli
  .version(pkg.version)

cli
  .command('list')
  .alias('ls')
  .describe('List keys in a bucket. Note: expects env vars for ACCESS_KEY_ID and SECRET_ACCESS_KEY to be set.')
  .option('-e, --endpoint', 'Bucket endpoint.')
  .option('-r, --region', 'Bucket region.')
  .option('-b, --bucket', 'Bucket name.')
  .option('-p, --prefix', 'Key prefix.')
  .option('-s, --start-after', 'Start listing after this key.')
  .action(async (/** @type {Record<string, string|undefined>} */ options) => {
    const accessKeyId = notNully(process.env, 'ACCESS_KEY_ID', 'missing environment variable')
    const secretAccessKey = notNully(process.env, 'SECRET_ACCESS_KEY', 'missing environment variable')
    const { endpoint } = options
    const region = notNully(options, 'region', 'missing required option')
    const bucket = notNully(options, 'bucket', 'missing required option')
    const prefix = options.prefix ?? ''
    const startAfter = options['start-after']
    const s3 = new S3Client({ region, endpoint, credentials: { accessKeyId, secretAccessKey } })

    /** @type {string|undefined} */
    let token
    await new ReadableStream({
      async pull (controller) {
        const cmd = new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, StartAfter: startAfter, MaxKeys: 1000, ContinuationToken: token })
        const res = await s3.send(cmd)
        for (const obj of res.Contents ?? []) {
          if (!obj.Key || !obj.Key.endsWith('.car')) continue
          controller.enqueue(`${JSON.stringify({ region, bucket, key: obj.Key })}\n`)
        }
        if (!res.IsTruncated) {
          return controller.close()
        }
        token = res.NextContinuationToken
      }
    }).pipeTo(Writable.toWeb(process.stdout))
  })

cli
  .command('hash [key]')
  .option('-e, --endpoint', 'Service endpoint.')
  .option('-r, --region', 'Bucket region.')
  .option('-b, --bucket', 'Bucket name.')
  .action(async (/** @type {string|undefined} */ key, /** @type {Record<string, string|undefined>} */ options) => {
    const endpoint = new URL(options.endpoint ?? notNully(process.env, 'SERVICE_ENDPOINT', 'missing required option'))
    if (key) {
      const region = notNully(options, 'region', 'missing required option')
      const bucket = notNully(options, 'bucket', 'missing required option')
      const { cid } = await hash(endpoint, region, bucket, key)
      return console.log(dagJSON.stringify({ region, bucket, key, cid }))
    }

    const source = /** @type {ReadableStream<Uint8Array>} */ (Readable.toWeb(process.stdin))
    await source
      .pipeThrough(/** @type {Parse<{ region?: string, bucket?: string, key: string }>} */ (new Parse()))
      .pipeThrough(new Parallel(concurrency, async item => {
        const region = item.region ?? notNully(options, 'region', 'missing required option')
        const bucket = item.bucket ?? notNully(options, 'bucket', 'missing required option')
        const { key } = item
        try {
          const { cid } = await retry(() => hash(endpoint, region, bucket, key))
          return { region, bucket, key, cid }
        } catch (err) {
          console.warn(`failed hash of ${region}/${bucket}/${key}`, err)
          return { region, bucket, key, error: err.message }
        }
      }))
      .pipeThrough(new TransformStream({
        transform: (item, controller) => controller.enqueue(`${dagJSON.stringify(item)}\n`)
      }))
      .pipeTo(Writable.toWeb(process.stdout))
  })

/**
 * @param {URL} endpoint
 * @param {string} region
 * @param {string} bucket
 * @param {string} key
 * @returns {Promise<{ cid: import('multiformats').Link }>}
 */
const hash = async (endpoint, region, bucket, key) => {
  const url = new URL(endpoint)
  url.searchParams.set('region', region)
  url.searchParams.set('bucket', bucket)
  url.searchParams.set('key', key)
  const res = await fetch(url)
  const text = await res.text()
  if (!res.ok) throw new Error(`hash failed: ${text}`)
  return dagJSON.parse(text)
}

cli.command('copy [key] [cid]')
  .option('--root', 'DAG root CID (if not derivable from key).')
  .option('-e, --endpoint', 'Service endpoint.')
  .option('-r, --region', 'Bucket region.')
  .option('-b, --bucket', 'Bucket name.')
  .action(async (/** @type {string|undefined} */ key, /** @type {string|undefined} */ cidstr, /** @type {Record<string, string|undefined>} */ options) => {
    const endpoint = new URL(options.endpoint ?? notNully(process.env, 'COPY_SERVICE_ENDPOINT', 'missing required option'))
    if (key && cidstr) {
      const region = notNully(options, 'region', 'missing required option')
      const bucket = notNully(options, 'bucket', 'missing required option')
      const root = options.root ? Link.parse(options.root) : bucketKeyToRootCID(key)
      if (!root) throw new Error('missing required option: root')
      const cid = Link.parse(cidstr)
      try {
        // @ts-expect-error
        await copy(endpoint, region, bucket, key, cid, root)
        return console.log(dagJSON.stringify({ region, bucket, key, cid, root }))
      } catch (err) {
        console.warn(`failed copy of ${region}/${bucket}/${key}`, err)
        return console.log(dagJSON.stringify({ region, bucket, key, cid, root, error: err.message }))
      }
    }

    const source = /** @type {ReadableStream<Uint8Array>} */ (Readable.toWeb(process.stdin))
    await source
      .pipeThrough(/** @type {Parse<{ region?: string, bucket?: string, key: string, cid: { '/': string }, root?: { '/': string } }>} */ (new Parse()))
      .pipeThrough(new Parallel(concurrency, async item => {
        const region = item.region ?? notNully(options, 'region', 'missing required option')
        const bucket = item.bucket ?? notNully(options, 'bucket', 'missing required option')
        const { key } = item
        const cid = Link.parse(item.cid['/'])
        const root = item.root
          ? Link.parse(item.root['/'])
          : options.root
          ? Link.parse(options.root)
          : bucketKeyToRootCID(key)
        if (!root) throw new Error('missing required option: root')
        try {
          // @ts-expect-error
          await retry(() => copy(endpoint, region, bucket, key, cid, root))
          return { region, bucket, key, cid, root }
        } catch (err) {
          console.warn(`failed copy of ${region}/${bucket}/${key}`, err)
          return { region, bucket, key, cid, root, error: err.message }
        }
      }))
      .pipeThrough(new TransformStream({
        transform: (item, controller) => controller.enqueue(`${dagJSON.stringify(item)}\n`)
      }))
      .pipeTo(Writable.toWeb(process.stdout))
  })

/**
 * @param {URL} endpoint
 * @param {string} region
 * @param {string} bucket
 * @param {string} key
 * @param {import('multiformats').Link} shard
 * @param {import('multiformats').UnknownLink} root
 */
const copy = async (endpoint, region, bucket, key, shard, root) => {
  const url = new URL(endpoint)
  url.searchParams.set('region', region)
  url.searchParams.set('bucket', bucket)
  url.searchParams.set('key', key)
  url.searchParams.set('shard', shard.toString())
  url.searchParams.set('root', root.toString())
  const res = await fetch(url)
  const text = await res.text()
  if (!res.ok) throw new Error(`copy failed: ${text}`)
  return dagJSON.parse(text)
}

/** @param {string} key */
const bucketKeyToRootCID = key => {
  if (key.startsWith('complete/')) {
    const cidstr = key.split('/').pop()?.replace('.car', '')
    if (!cidstr) return
    return cidstr ? Link.parse(cidstr) : undefined
  }
}

/**
 * @param {Record<string, string|undefined>} obj
 * @param {string} key
 */
const notNully = (obj, key, msg = 'unexpected null value') => {
  const value = obj[key]
  if (!value) throw new Error(`${msg}: ${key}`)
  return value
}

cli.parse(process.argv)
