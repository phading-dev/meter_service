import "./env";
import { ENV_VARS } from "../env_vars";
import { spawnSync } from "child_process";

async function main() {
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "deleteinstance",
    ENV_VARS.bigtableInstanceId,
  ]);
}

main();
