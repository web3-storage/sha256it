import { SSTConfig } from "sst";
import { Tags } from 'aws-cdk-lib'
import { API } from "./stacks/api";

export default {
  config(_input) {
    return {
      name: "sha256it",
      region: "us-west-2",
    };
  },
  stacks(app) {
    Tags.of(app).add('Project', 'sha256it')
    Tags.of(app).add('Repository', 'https://github.com/web3-storage/sha256it')
    Tags.of(app).add('Environment', `${app.stage}`)
    Tags.of(app).add('ManagedBy', 'SST')
    app.stack(API);
  }
} satisfies SSTConfig;
