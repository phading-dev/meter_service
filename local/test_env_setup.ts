import "./env";
import { ENV_VARS } from "../env_vars";
import { spawnSync } from "child_process";

async function main() {
  spawnSync("gcloud", ["auth", "application-default", "login"], {
    stdio: "inherit",
  });
  spawnSync("gcloud", ["config", "set", "project", ENV_VARS.projectId], {
    stdio: "inherit",
  });
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "createinstance",
    ENV_VARS.bigtableInstanceId,
    "Test instance",
    ENV_VARS.bigtableClusterId,
    ENV_VARS.bigtableZone,
    "1",
    "SSD",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createtable",
    ENV_VARS.bigtableDatabaseId,
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableDatabaseId,
    "w:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableDatabaseId,
    "a:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableDatabaseId,
    "s:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableDatabaseId,
    "u:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableDatabaseId,
    "t:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableDatabaseId,
    "c:maxversions=1",
  ]);
}

main();
