import { ENV_VARS } from "../env";
import { Bigtable } from "@google-cloud/bigtable";

export let BIGTABLE = new Bigtable({
  projectId: ENV_VARS.projectId,
})
  .instance(ENV_VARS.bigtableInstanceId)
  .table(ENV_VARS.bigtableDatabaseId);
