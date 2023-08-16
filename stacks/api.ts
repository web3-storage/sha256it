import { StackContext, Function } from "sst/constructs";

export function API({ stack }: StackContext) {
  stack.setDefaultFunctionProps({
    memorySize: '1 GB',
    runtime: 'nodejs18.x',
    architecture: 'arm_64',
    timeout: '15 minutes'
  })

  const fun = new Function(stack, 'fn', {
    handler: 'packages/functions/src/lambda.handler',
    url: { cors: true, authorizer: 'none' }
  })

  fun.attachPermissions(['s3:GetObject'])

  stack.addOutputs({ url: fun.url })
}
