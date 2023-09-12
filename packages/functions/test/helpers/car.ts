import crypto from 'node:crypto'
import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'
import { Readable, Writable } from 'node:stream'
import { CARReaderStream, CARWriterStream,  } from 'carstream'
import { Block as CARBlock, Position } from 'carstream/api'
import * as Link from 'multiformats/link'
import { sha256 } from 'multiformats/hashes/sha2'
import * as raw from 'multiformats/codecs/raw'
import * as Block from 'multiformats/block'
import * as Digest from 'multiformats/hashes/digest'
import { customAlphabet } from 'nanoid'
import { MultihashIndexSortedWriter } from 'cardex/multihash-index-sorted'

const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)

export const generateTestCAR = async (targetSize: number) => {
  const car = await testCAR(targetSize)
  const carPath = path.join(os.tmpdir(), `${id()}.car`)
  await car.body.pipeTo(Writable.toWeb(fs.createWriteStream(carPath)))
  const cid = await carHash(carPath)
  const size = await carSize(carPath)
  console.log(`generated test CAR: ${cid}`)
  console.log(`  root: ${car.root}`)
  console.log(`  size: ${size} bytes`)
  console.log(`  path: ${carPath}`)
  return { cid, root: car.root, size, path: carPath }
}

export const writeCARIndex = async (src: string, dest: string) => {
  const { readable, writable } = new TransformStream()
  const writer = MultihashIndexSortedWriter.createWriter({ writer: writable.getWriter() })
  const stream = Readable.toWeb(fs.createReadStream(src)) as ReadableStream<Uint8Array>
  const items: Array<{ cid: Link.UnknownLink } & Position> = []
  await Promise.all([
    stream
      .pipeThrough(new CARReaderStream())
      .pipeTo(new WritableStream({
        async write (block) {
          items.push({ cid: block.cid, offset: block.offset, length: block.length })
          await writer.add(block.cid, block.offset)
        },
        close () {
          return writer.close()
        }
      })),
    readable.pipeTo(Writable.toWeb(fs.createWriteStream(dest)))
  ])
  const stats = await fs.promises.stat(dest)
  console.log(`wrote CAR index for: ${src}`)
  console.log(`  size: ${stats.size} bytes`)
  console.log(`  path: ${dest}`)
  return { path: dest, size: stats.size, items }
}

const randomBlock = async () => {
  const bytes = crypto.randomBytes(randomInt(1, 1024 * 1024 * 2))
  const cid = Link.create(raw.code, await sha256.digest(bytes))
  // @ts-expect-error
  const block = Block.createUnsafe({ bytes, cid, codec: raw })
  return block
}

const testCAR = async (targetSize: number) => {
  const root = await randomBlock()
  let total = root.bytes.length

  const src = new ReadableStream({
    async pull (controller) {
      const block = await randomBlock()
      total += block.bytes.length
      controller.enqueue(block)
      if (total >= targetSize) {
        controller.close()
      }
    }
  })

  return { root: root.cid, body: src.pipeThrough(new CARWriterStream([root.cid])) }
}

const carHash = async (path: string) => {
  const hasher = crypto.createHash('sha256')
  await Readable.toWeb(fs.createReadStream(path))
    .pipeTo(new WritableStream({ write: chunk => { hasher.update(chunk) } }))
  const digest = Digest.create(sha256.code, hasher.digest())
  return Link.create<Uint8Array, 0x0202, typeof sha256.code>(0x0202, digest)
}

const carSize = async (path: string) => {
  const stat = await fs.promises.stat(path)
  return stat.size
}

const randomInt = (min: number, max: number) => {
  min = Math.ceil(min)
  max = Math.floor(max)
  return Math.floor(Math.random() * (max - min) + min)
}
