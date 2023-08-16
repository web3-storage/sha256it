#!/usr/bin/env node
import fs from 'node:fs'
import { Readable, Writable } from 'node:stream'
import sade from 'sade'
import { S3Client, ListObjectsV2Command } from '@aws-sdk/client-s3'
import dotenv from 'dotenv'
import { Parse } from 'ndjson-web'
import * as dagJSON from '@ipld/dag-json'
import { Parallel } from 'parallel-transform-web'

const pkg = JSON.parse(fs.readFileSync(new URL('./package.json', import.meta.url)).toString())
const cli = sade(pkg.name)
const concurrency = 10

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
  .action(async (/** @type {Record<string, string|undefined>} */ options) => {
    const accessKeyId = notNully(process.env, 'ACCESS_KEY_ID', 'missing environment variable')
    const secretAccessKey = notNully(process.env, 'SECRET_ACCESS_KEY', 'missing environment variable')
    const { endpoint } = options
    const region = notNully(options, 'region', 'missing required option')
    const bucket = notNully(options, 'bucket', 'missing required option')
    const prefix = options.prefix ?? ''
    const s3 = new S3Client({ region, endpoint, credentials: { accessKeyId, secretAccessKey } })

    /** @type {string|undefined} */
    let token
    await new ReadableStream({
      async pull (controller) {
        const cmd = new ListObjectsV2Command({ Bucket: bucket, Prefix: prefix, MaxKeys: 1000, ContinuationToken: token })
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
    const endpoint = new URL(process.env.SERVICE_ENDPOINT ?? notNully(options, 'endpoint', 'missing required option'))
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
        const { cid } = await hash(endpoint, region, bucket, key)
        return { region, bucket, key, cid }
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
