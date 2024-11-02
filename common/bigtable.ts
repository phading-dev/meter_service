import { INSTANCE_ID, PROJECT_ID, TABLE_ID } from "./env_vars";
import { Bigtable } from "@google-cloud/bigtable";

export let BIGTABLE = new Bigtable({
  projectId: PROJECT_ID,
})
  .instance(INSTANCE_ID)
  .table(TABLE_ID);
