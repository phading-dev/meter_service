import { ENV_VARS } from "./env";
import { spawnSync } from "child_process";
import "./env_local";

async function main() {
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "deleteinstance",
    ENV_VARS.bigtableInstanceId,
  ]);
}

main();
