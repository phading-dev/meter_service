import "../env_const";
import "@phading/cluster/dev/env";
import { ENV_VARS } from "../env_vars";

ENV_VARS.bigtableInstanceId = ENV_VARS.singleBigtableInstanceId;
ENV_VARS.replicas = 1;
ENV_VARS.cpu = "400m";
ENV_VARS.memory = "512Mi";
