import "./env";
import { ENV_VARS } from "../env_vars";
import { spawnSync } from "child_process";
import { existsSync } from "fs";

async function main() {
  if (
    existsSync(
      `${process.env.HOME}/.config/gcloud/application_default_credentials.json`,
    )
  ) {
    console.log("Application default credentials already exist.");
  } else {
    spawnSync("gcloud", ["auth", "application-default", "login"], {
      stdio: "inherit",
    });
  }
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
    ENV_VARS.bigtableTableId,
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableTableId,
    "w:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableTableId,
    "a:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableTableId,
    "s:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableTableId,
    "u:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableTableId,
    "t:maxversions=1",
  ]);
  spawnSync("cbt", [
    "-project",
    ENV_VARS.projectId,
    "-instance",
    ENV_VARS.bigtableInstanceId,
    "createfamily",
    ENV_VARS.bigtableTableId,
    "c:maxversions=1",
  ]);
}

main();
