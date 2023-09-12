import { GenericContainer, StartedTestContainer } from 'testcontainers'
import { customAlphabet } from 'nanoid'
import { S3Client, CreateBucketCommand, HeadObjectCommand } from '@aws-sdk/client-s3'
import { CreateTableCommand, DynamoDBClient, GetItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall } from '@aws-sdk/util-dynamodb'

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

export async function createDynamo (opts: { port?: number, region?: string } = {}) {
  console.log('Creating local DynamoDB...')
  const port = opts?.port ?? 8000
  const region = opts?.region ?? 'us-west-2'
  const container = await new GenericContainer('amazon/dynamodb-local:latest')
    .withExposedPorts(port)
    .start()

  const clientOpts = {
    endpoint: `http://127.0.0.1:${container.getMappedPort(8000)}`,
    region,
    credentials
  }

  return { container, client: new DynamoDBClient(clientOpts), ...clientOpts }
}

export async function createDynamoTable (dynamo: DynamoDBClient, pfx = '') {
  const name = (pfx ? `${pfx}-` : '') + id()
  console.log(`Creating DynamoDB table "${name}"...`)

  await dynamo.send(
    new CreateTableCommand({
      TableName: name,
      AttributeDefinitions: [
        { AttributeName: 'blockmultihash', AttributeType: 'S' },
        { AttributeName: 'carpath', AttributeType: 'S' }
      ],
      KeySchema: [
        { AttributeName: 'blockmultihash', KeyType: 'HASH' },
        { AttributeName: 'carpath', KeyType: 'RANGE' }
      ],
      ProvisionedThroughput: {
        ReadCapacityUnits: 1,
        WriteCapacityUnits: 1
      }
    })
  )

  return name
}

export const itemExists = async <T>(dynamo: DynamoDBClient, tableName: string, key: T) => {
  const cmd = new GetItemCommand({
    TableName: tableName,
    Key: marshall(key)
  })
  const res = await dynamo.send(cmd)
  return Boolean(res.Item)
}
