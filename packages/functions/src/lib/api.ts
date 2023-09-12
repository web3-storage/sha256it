import { Link, Version } from 'multiformats'
import { CAR_CODEC } from './constants.js'

export type ShardLink = Link<Uint8Array, typeof CAR_CODEC>

export interface Endpoint {
  endpoint?: string
}

export interface Credentials {
  credentials?: {
    accessKeyId: string,
    secretAccessKey: string
  }
}

export interface ObjectID extends Endpoint, Credentials {
  region: string
  bucket: string
  key: string
}

export interface ContentAddressedObjectID<
  Data extends unknown = unknown,
  Format extends number = number,
  Alg extends number = number,
  V extends Version = 1
> extends ObjectID {
  cid: Link<Data, Format, Alg, V>
}

export interface ShardObjectID extends ContentAddressedObjectID<Uint8Array, typeof CAR_CODEC> {}
