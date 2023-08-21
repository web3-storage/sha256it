export const notNully = (obj: Record<string, string|undefined>, k: string, msg = 'unexpected null value') => {
  const v = obj[k]
  if (!v) throw new Error(`${msg}: ${k}`)
  return v
}

export const mustGetEnv = (env: Record<string, string|undefined>, k: string) => notNully(env, k, 'missing enviornment variable')

export const errorResponse = (message: string, statusCode = 500) => ({
  statusCode,
  body: JSON.stringify({ ok: false, error: message })
})
