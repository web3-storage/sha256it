# sha256it

Tools to calculates sha256 CAR CID for an object in S3 and copy it to R2 _(or other S3 compatible api)_

Lambdas to hash and copy CARS are deployed via SST and seed.run.

The `sha256it` cli uses those lambdas to do the work.

## Getting started

The repo contains the infra deployment code and a cli to use it.

```
├── packages
|   ├── cli        - sha256it cli to hash, copy, and verify cars
|   └── functions  - lambdas for hashing and copying CARs
└── stacks         - sst and aws cdk code to deploy the lambdas
```

To work on this codebase **you need**:

- Node.js >= v18 (prod env is node v18)
- Install the deps with `npm i`

You can then run the tests locally with `npm test`. 

## Usage

Commands for the cli, defined in the `packages/cli` directory.

### list

Fetch a list of keys, partitioned by starting key with the `--start-after` flag

```shell
sha256it list --region us-west-2 --bucket bucketname --prefix complete \
> keys.ndjson
```

- `ACCESS_KEY_ID` and `SECRET_ACCESS_KEY` must be set in env

**output**

```json
{"region":"us-west-2","bucket":"[bucket-name]","key":"complete/[root cid].car"}
```

### hash

Hash a list of keys. 

```shell
sha256it hash --endpoint https://???.lambda-url.us-west-2.on.aws/ \
< keys.ndjson \
> hashed.ndjson
```

- `--endpoint` is the function url of the `hash` lambda

**output**

```json
{"bucket":"[bucket name]","cid":{"/":"[car cid]"},"key":"complete/[root cid].car","region":"us-west-2"}
```

### copy

Copy CARs from source to dest. 
_note: Get the function url of the copy lambda from aws console._

```shell
sha256it copy --endpoint https://!!!.lambda-url.us-west-2.on.aws/ \
< hashed.ndjson \
> copied.ndjson
```

- `--endpoint` is the function url of the `copy` lambda

### head

Check the head response for a car cid at a bucket endpoint

```shell
sha256it head --region auto --bucket carpark --endpoint https://<ACCOUNT_ID>.r2.cloudflarestorage.com \
< copied.ndjson \
> verified.ndjson
```

- `DEST_ACCESS_KEY_ID` and `DEST_SECRET_ACCESS_KEY` must be set in env
- `--endpoint` is the s3 compatible api url


**output**

```json
{"bucket":"carpark","cid":{"/":"[car cid]"},"key":"[car cid]/[car cid].car","length":10862134,"region":"auto","status":200}
```
