import { StackContext, Function, Config } from 'sst/constructs'
import { mustGetEnv } from '../packages/functions/src/lib/util'

export function API ({ stack }: StackContext) {
  const DEST_ENDPOINT = mustGetEnv(process.env, 'DEST_ENDPOINT')
  const DEST_REGION = mustGetEnv(process.env, 'DEST_REGION')
  const CARPARK_BUCKET = mustGetEnv(process.env, 'CARPARK_BUCKET')
  const SATNAV_BUCKET = mustGetEnv(process.env, 'SATNAV_BUCKET')
  const DUDEWHERE_BUCKET = mustGetEnv(process.env, 'DUDEWHERE_BUCKET')

  stack.setDefaultFunctionProps({
    memorySize: '1 GB',
    runtime: 'nodejs18.x',
    architecture: 'arm_64',
    timeout: '15 minutes'
  })

  const hashFunction = new Function(stack, 'hash', {
    handler: 'packages/functions/src/hash.handler',
    url: { cors: true, authorizer: 'none' }
  })

  hashFunction.attachPermissions(['s3:GetObject'])

  const accessKeyID = new Config.Secret(stack, 'DEST_ACCESS_KEY_ID')
  const secretAccessKey = new Config.Secret(stack, 'DEST_SECRET_ACCESS_KEY')

  const copyFunction = new Function(stack, 'copy', {
    handler: 'packages/functions/src/copy.handler',
    url: { cors: true, authorizer: 'none' },
    environment: {
      DEST_ENDPOINT,
      DEST_REGION,
      CARPARK_BUCKET,
      SATNAV_BUCKET,
      DUDEWHERE_BUCKET
    },
    bind: [accessKeyID, secretAccessKey]
  })

  copyFunction.attachPermissions(['s3:GetObject'])

  stack.addOutputs({
    hashFunctionURL: hashFunction.url,
    copyFunctionURL: copyFunction.url
  })
}
