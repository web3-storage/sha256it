import { GenericContainer, StartedTestContainer } from 'testcontainers'
import { customAlphabet } from 'nanoid'
import { S3Client, CreateBucketCommand, HeadObjectCommand } from '@aws-sdk/client-s3'

const id = customAlphabet('1234567890abcdefghijklmnopqrstuvwxyz', 10)
const credentials = { accessKeyId: 'minioadmin', secretAccessKey: 'minioadmin' }

export interface TestAWSService<T> {
  client: T
  container: StartedTestContainer
  region: string
  endpoint: string
  credentials: { accessKeyId: string, secretAccessKey: string }
}

export const createS3 = async (opts: { port?: number, region?: string } = {}): Promise<TestAWSService<S3Client>> => {
  console.log('Creating local S3...')
  const region = opts.region || 'us-west-2'
  const port = opts.port || 9000

  const container = await new GenericContainer('quay.io/minio/minio')
    .withCommand(['server', '/data'])
    .withExposedPorts(port)
    .start()

  const clientOpts = {
    endpoint: `http://127.0.0.1:${container.getMappedPort(port)}`,
    forcePathStyle: true,
    region,
    credentials
  }

  return { container, client: new S3Client(clientOpts), ...clientOpts }
}

export const createS3Bucket = async (s3: S3Client, pfx = '') => {
  const name = (pfx ? `${pfx}-` : '') + id()
  console.log(`Creating S3 bucket "${name}"...`)
  await s3.send(new CreateBucketCommand({ Bucket: name }))
  return name
}

export const keyExists = async (s3: S3Client, bucket: string, key: string) => {
  try {
    await s3.send(new HeadObjectCommand({ Bucket: bucket, Key: key }))
    return true
  } catch (err: any) {
    if (err.$metadata?.httpStatusCode !== 404) {
      console.error(err)
      throw new Error('failed head request', { cause: err })
    }
    return false
  }
}
